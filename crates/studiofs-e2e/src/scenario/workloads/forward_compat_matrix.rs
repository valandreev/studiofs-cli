//! Supported-release forward-compatibility integration and mounted E2E matrix (SFS-1019).
//!
//! Runs the 0.2.5 baseline binary and the compiled, versioned `test-successor`
//! binary (SFS-1131) — plus the published `v0.2.4` binary for the fenced-client
//! upgrade cell — against isolated Compose PostgreSQL + RustFS services with a
//! dedicated local `redb` database per client.
//!
//! Publishes `forward-compat-matrix.json` in the scenario artifact directory
//! recording binary SHA-256 digests, format/PostgreSQL schema versions,
//! event/checkpoint protocol versions, declared successor changes, and
//! pass/fail/skipped outcomes per cell and operation.

use std::collections::BTreeSet;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use studiofs_meta::schema::{DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE, ROOT_INO, ResolvedSliceSpan};
use studiofs_meta::{MetaInodes, MetaNamespace, MetaPayloadObjects, RedbMetaEngine};

use super::recoverable_trash_janitor::{pg_observation_error_kind, with_pg, with_pg_timeout};
use super::snapshot_v2::{
    parse_snapshot_id, redact_diagnostics, snapshot_create_args, snapshot_mount_args,
};
use super::*;
use crate::client::{
    ClientCommandOutput, ClientControllerConfig, ClientControllerError,
    available_snapshot_mount_point, client_daemon_log_paths, mount_point_is_active,
};
use crate::infra::{CommandExecutor, CommandSpec, SystemCommandExecutor};
use crate::mounted_tree::hex_lower;
use crate::process_job::{ChildJob, wait_for_child_exit};

/// Registered scenario name for the forward-compatibility matrix.
pub const MOUNTED_FORWARD_COMPAT_MATRIX: &str = "mounted-forward-compat-matrix";
/// Shorthand alias for the forward-compatibility matrix.
pub const FORWARD_COMPAT_MATRIX_ALIAS: &str = "forward-compat-matrix";

const MATRIX_ARTIFACT: &str = "forward-compat-matrix.json";
const SUCCESSOR_BIN_ENV: &str = "STUDIOFS_E2E_SUCCESSOR_BIN";
const SUCCESSOR_MANIFEST_ENV: &str = "STUDIOFS_E2E_SUCCESSOR_MANIFEST";
const V024_BIN_ENV: &str = "STUDIOFS_E2E_V024_BIN";
const V022_BIN_ENV: &str = "STUDIOFS_E2E_V022_BIN";
const REQUIRE_V024_ENV: &str = "STUDIOFS_E2E_REQUIRE_V024";
const CRASH_ROLLBACK_ENV: &str = "STUDIOFS_E2E_CRASH_ROLLBACK";
const DEFAULT_SUCCESSOR_MANIFEST: &str = "target/test-successor/manifest.json";
const EXPECTED_SUCCESSOR_MARKER: &str = "STUDIOFS-TEST-SUCCESSOR-BUILD";
const SUCCESSOR_XATTR_CAPABILITY: &str = "event.set_user_xattr.v1";
const FORMAT_7_REFUSAL: &str = "unsupported metadata/pg sync format version: found 7, expected 6";

const REQUIRED_CHANGE_KINDS: [&str; 4] = [
    "postgres_migration",
    "event_kind",
    "checkpoint_format",
    "local_format",
];
const REQUIRED_ROLLBACK_CLASSES: [&str; 2] = ["reversible", "irreversible"];

const CLIENT_SUCCESSOR: &str = "client-successor";
const CLIENT_BASELINE: &str = "client-baseline";
const CLIENT_COLD_BASELINE: &str = "client-cold-baseline";
const CLIENT_COLD_SUCCESSOR: &str = "client-cold-successor";
const CLIENT_BASELINE_PROBE: &str = "client-baseline-probe";

const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(90);
const POLL_INTERVAL: Duration = Duration::from_millis(250);
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(120);
const EXPECTED_FAIL_TIMEOUT: Duration = Duration::from_secs(30);
const MULTI_CHUNK_BYTES: usize = DEFAULT_CHUNK_SIZE as usize + DEFAULT_BLOCK_SIZE as usize;
const PRE_MIGRATION_PAYLOAD: &[u8] = b"published before the interrupted core migration\n";

fn crash_rollback_enabled() -> bool {
    env::var(CRASH_ROLLBACK_ENV).as_deref() == Ok("1")
}

fn recovered_payload_matches(actual: &[u8], expected: &[u8]) -> bool {
    Sha256::digest(actual) == Sha256::digest(expected)
}

fn exercise_migration_restart(
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
    config: &ClientControllerConfig,
    binaries: &MatrixBinaries,
    ops: &mut Vec<OperationRecord>,
) -> Result<(), ScenarioError> {
    controller.set_client_bin_override(CLIENT_SUCCESSOR, binaries.baseline_bin.clone())?;
    controller.init_first_client()?;
    controller.mount(CLIENT_SUCCESSOR)?;
    let mount = &controller.client_by_name(CLIENT_SUCCESSOR)?.mount_point;
    let before_write = observe_metadata_version(config, Duration::from_secs(15))?;
    write_sync(&mount.join("before-migration.txt"), PRE_MIGRATION_PAYLOAD)?;
    wait_for_published_write(controller, CLIENT_SUCCESSOR, config, before_write)?;
    controller.unmount(CLIENT_SUCCESSOR)?;
    let before = observe_shared_state(config)?;
    controller.set_client_bin_override(CLIENT_SUCCESSOR, binaries.successor_bin.clone())?;

    // The trigger runs after the migration SQL and marker update in the same
    // transaction. A cancelled statement must roll both back.
    matrix_sql(
        config,
        "install core migration cancellation",
        "CREATE FUNCTION sfs1020_cancel_core() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.component = 'core' AND NEW.version = 2 THEN
                 RAISE EXCEPTION USING ERRCODE = '57014', MESSAGE = 'SFS-1020 core cancellation';
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER sfs1020_cancel_core AFTER UPDATE ON studiofs_schema_components
         FOR EACH ROW EXECUTE FUNCTION sfs1020_cancel_core();",
    )?;
    let args = vec!["upgrade".into(), config.volume_name.clone()];
    let interrupted =
        controller.run_cli_allow_failure(CLIENT_SUCCESSOR, "cancel-core-migration", args.clone());
    let remove_fault = matrix_sql(
        config,
        "remove core migration cancellation",
        "DROP TRIGGER sfs1020_cancel_core ON studiofs_schema_components;
         DROP FUNCTION sfs1020_cancel_core();",
    );
    remove_fault?;
    assert_refusal("cancelled core migration", &interrupted?, &["57014"])?;
    with_pg_timeout(
        config,
        "verify migration rollback",
        Some(Duration::from_secs(15)),
        |client| async move {
            let row = client
                .query_one(
                    "SELECT version, to_regclass('studiofs_successor_probe') IS NULL
             FROM studiofs_schema_components WHERE component = 'core'",
                    &[],
                )
                .await
                .map_err(pg_err("verify migration rollback"))?;
            if row.get::<_, i32>(0) != 1 || !row.get::<_, bool>(1) {
                return Err(io::Error::other(
                    "cancelled core migration changed the registry or left its table",
                )
                .into());
            }
            Ok(())
        },
    )?;
    assert_shared_state_unchanged(config, &before, "cancelled core migration")?;
    controller.run_cli(CLIENT_SUCCESSOR, "retry-core-migration", args.clone())?;
    assert_shared_state_unchanged(config, &before, "retried core migration")?;
    let local_args = [args, vec!["--local-only".into()]].concat();
    let mut env = fast_sync_env();
    env.push((
        "STUDIOFS_E2E_EXIT_AFTER_LOCAL_ACTIVATION".into(),
        "1".into(),
    ));
    controller.set_client_env_overrides(CLIENT_SUCCESSOR, env)?;
    let exited = controller.run_cli_allow_failure(
        CLIENT_SUCCESSOR,
        "interrupt-local-revision-after-activation",
        local_args.clone(),
    );
    controller.set_client_env_overrides(CLIENT_SUCCESSOR, fast_sync_env())?;
    if exited?.status != 86 {
        return Err(io::Error::other(
            "test-successor did not exit at the irreversible local revision marker",
        )
        .into());
    }
    assert_shared_state_unchanged(config, &before, "interrupted local revision")?;
    controller.run_cli(CLIENT_SUCCESSOR, "retry-local-revision", local_args)?;
    record_op(
        ops,
        "local_revision_restart_after_irreversible_marker",
        CellStatus::Passed,
        "test-successor exited before marker cleanup and completed the local upgrade in a new process",
    );
    record_op(
        ops,
        "core_migration_cancellation_and_retry",
        CellStatus::Passed,
        "a cancelled core=2 transaction left core=1 and no probe table; a new CLI process completed the migration",
    );
    Ok(())
}

fn matrix_sql(
    config: &ClientControllerConfig,
    operation: &str,
    sql: &'static str,
) -> Result<(), ScenarioError> {
    with_pg_timeout(
        config,
        operation,
        Some(Duration::from_secs(15)),
        |client| async move { client.batch_execute(sql).await.map_err(pg_err(operation)) },
    )
}

fn exercise_legacy_adoption_cancellation(
    controller: &MultiClientController<SystemClientProcessExecutor>,
    config: &ClientControllerConfig,
    client_name: &str,
    ops: &mut Vec<OperationRecord>,
) -> Result<(), ScenarioError> {
    let before = observe_protocol_state(config)?;
    matrix_sql(
        config,
        "install format-7 adoption cancellation",
        "CREATE FUNCTION sfs1020_cancel_adoption() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             RAISE EXCEPTION USING ERRCODE = '57014', MESSAGE = 'SFS-1020 adoption cancellation';
         END $$;
         CREATE TRIGGER sfs1020_cancel_adoption AFTER UPDATE OF metadata_format_version
         ON metadata_state FOR EACH ROW WHEN (OLD.metadata_format_version = 6 AND NEW.metadata_format_version = 7)
         EXECUTE FUNCTION sfs1020_cancel_adoption();",
    )?;
    let interrupted = controller.run_cli_allow_failure(
        client_name,
        "cancel-format7-adoption",
        vec!["upgrade".into(), config.volume_name.clone()],
    );
    matrix_sql(
        config,
        "remove format-7 adoption cancellation",
        "DROP TRIGGER sfs1020_cancel_adoption ON metadata_state;
         DROP FUNCTION sfs1020_cancel_adoption();",
    )?;
    assert_refusal(
        "cancelled format-7 adoption",
        &interrupted?,
        &["57014", "upgrade shared format"],
    )?;
    let after = observe_protocol_state(config)?;
    if after.metadata_format_version != Some(6)
        || after.policy_epoch != before.policy_epoch
        || after.checkpoint_version != before.checkpoint_version
        || after.event_log_prune_floor != before.event_log_prune_floor
    {
        return Err(io::Error::other(
            "cancelled adoption changed the legacy volume's serving state",
        )
        .into());
    }
    record_op(
        ops,
        "format7_adoption_cancellation",
        CellStatus::Passed,
        "the failed upgrade preserved format 6, its absent policy epoch, checkpoint version, and prune floor",
    );
    Ok(())
}

const INSTALL_CHECKPOINT_AND_PRUNE_FAULTS: &str = "CREATE SEQUENCE sfs1020_checkpoint_attempt;
     CREATE SEQUENCE sfs1020_prune_attempt;
     CREATE FUNCTION sfs1020_cancel_maintenance() RETURNS trigger LANGUAGE plpgsql AS $$
     BEGIN
         IF NEW.checkpoint_generation > OLD.checkpoint_generation THEN
             PERFORM nextval('sfs1020_checkpoint_attempt');
             RAISE EXCEPTION USING ERRCODE = '57014', MESSAGE = 'SFS-1020 checkpoint cancellation';
         END IF;
         IF NEW.event_log_prune_floor > OLD.event_log_prune_floor THEN
             PERFORM nextval('sfs1020_prune_attempt');
             RAISE EXCEPTION USING ERRCODE = '57014', MESSAGE = 'SFS-1020 prune cancellation';
         END IF;
         RETURN NEW;
     END $$;
     CREATE TRIGGER sfs1020_cancel_checkpoint AFTER UPDATE ON metadata_state
     FOR EACH ROW WHEN (NEW.checkpoint_generation > OLD.checkpoint_generation)
     EXECUTE FUNCTION sfs1020_cancel_maintenance();
     CREATE TRIGGER sfs1020_cancel_prune AFTER UPDATE ON metadata_state
     FOR EACH ROW WHEN (NEW.event_log_prune_floor > OLD.event_log_prune_floor)
     EXECUTE FUNCTION sfs1020_cancel_maintenance();";

fn wait_for_cancelled_transaction(
    config: &ClientControllerConfig,
    description: &str,
    sql: &'static str,
) -> Result<(), ScenarioError> {
    wait_for_matrix_state(description, CONVERGENCE_TIMEOUT, |deadline| {
        with_pg_timeout(
            config,
            description,
            Some(remaining_observation_time(deadline)),
            |client| async move {
                let row = client
                    .query_one(sql, &[])
                    .await
                    .map_err(pg_err("read cancellation counter"))?;
                Ok(row.get(0))
            },
        )
    })
}

pub(in crate::scenario) struct MountedForwardCompatMatrix;

impl Scenario for MountedForwardCompatMatrix {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: MOUNTED_FORWARD_COMPAT_MATRIX,
            description: "SFS-1019 supported-release forward-compatibility mounted E2E matrix: 0.2.5 baseline + compiled test-successor + fenced published v0.2.4",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        let compose = compose_control_from_manifest(context)?;
        let config = context
            .client_controller_config
            .clone()
            .ok_or(ScenarioError::MissingComposeInfrastructure)?;

        let binaries = resolve_matrix_binaries(&config.studiofs_bin)?;
        let baseline_id = query_version_identities(&binaries.baseline_bin, "0.2.5-baseline")?;
        let successor_id =
            query_version_identities(&binaries.successor_bin, "compiled-test-successor")?;
        verify_successor_contract(&baseline_id, &successor_id)?;

        // Run the published v0.2.4 -> 0.2.5 adoption/fencing pair first while the Compose
        // PostgreSQL database is still at core schema <= 1, before the compiled test-successor
        // applies its irreversible core schema v2 migration.
        let (v024_pair, v024_cleanup_failed) = run_fenced_v024_pair(&config, &binaries);
        let v022_pair = if crash_rollback_enabled() && !v024_cleanup_failed {
            env::var_os(V022_BIN_ENV)
                .map(PathBuf::from)
                .map(|legacy| run_format5_adoption_pair(&config, &binaries, &baseline_id, &legacy))
                .transpose()?
        } else {
            None
        };
        let legacy_cleanup_failed = v024_cleanup_failed
            || v022_pair
                .as_ref()
                .is_some_and(|(_, cleanup_failed)| *cleanup_failed);

        let mut primary_pair = ReleasePairReport {
            pair_id: "0.2.5-baseline-vs-compiled-test-successor".to_string(),
            baseline: baseline_id,
            successor: successor_id,
            observed_protocol: ObservedProtocolState::default(),
            cells: Vec::new(),
        };

        let primary_result = if legacy_cleanup_failed {
            Err(
                io::Error::other("primary pair skipped after legacy adoption cleanup failure")
                    .into(),
            )
        } else {
            run_baseline_successor_pair(context, &config, &compose, &binaries, &mut primary_pair)
        };

        let mut pairs = vec![primary_pair, v024_pair];
        if let Some((pair, _)) = v022_pair {
            pairs.push(pair);
        }
        let summary = MatrixSummary::from_pairs(&pairs);
        let passed = primary_result.is_ok() && summary.failed == 0;
        let report = ForwardCompatMatrixReport {
            schema_version: 1,
            scenario: MOUNTED_FORWARD_COMPAT_MATRIX.to_string(),
            platform: current_platform_label().to_string(),
            generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            source_commit: binaries.source_commit,
            source_dirty: binaries.source_dirty,
            pairs,
            future_pairs_slot: "Reserved for future released pairs such as 0.2.5/0.2.7 (SFS-1053)."
                .to_string(),
            summary,
            passed,
        };
        write_matrix_report(&context.scenario_dir.join(MATRIX_ARTIFACT), &report)?;

        primary_result.map_err(|error| io::Error::other(matrix_error_detail(&config, &error)))?;
        if !report.passed {
            return Err(io::Error::other(format!(
                "forward-compatibility matrix reported {} failed cell(s); inspect {}",
                report.summary.failed, MATRIX_ARTIFACT
            ))
            .into());
        }
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let path = context.scenario_dir.join(MATRIX_ARTIFACT);
        let bytes = fs::read(&path)?;
        let report: ForwardCompatMatrixReport = serde_json::from_slice(&bytes)?;
        let issues = validate_matrix_report(&report);
        if !issues.is_empty() {
            return Err(io::Error::other(format!(
                "forward-compatibility matrix validation failed: {}",
                issues.join("; ")
            ))
            .into());
        }
        Ok(vec![format!(
            "artifacts/scenarios/{MOUNTED_FORWARD_COMPAT_MATRIX}/{MATRIX_ARTIFACT}"
        )])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct MatrixBinaries {
    baseline_bin: PathBuf,
    successor_bin: PathBuf,
    v024_bin: Option<PathBuf>,
    require_v024: bool,
    source_commit: Option<String>,
    source_dirty: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CellStatus {
    Passed,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OperationRecord {
    pub(crate) name: String,
    pub(crate) status: CellStatus,
    pub(crate) detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MatrixCellReport {
    pub(crate) id: String,
    pub(crate) category: String,
    pub(crate) status: CellStatus,
    pub(crate) duration_ms: u64,
    pub(crate) detail: String,
    pub(crate) operations: Vec<OperationRecord>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SchemaComponentVersion {
    pub(crate) component: String,
    pub(crate) version: i32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ObservedProtocolState {
    pub(crate) metadata_format_version: Option<i16>,
    pub(crate) write_requirement: Option<i16>,
    pub(crate) policy_epoch: Option<i64>,
    pub(crate) checkpoint_version: Option<i64>,
    pub(crate) event_log_prune_floor: Option<i64>,
    pub(crate) schema_components: Vec<SchemaComponentVersion>,
    pub(crate) checkpoint_formats_observed: Vec<i16>,
    pub(crate) event_types_observed: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BinaryIdentityRecord {
    pub(crate) label: String,
    pub(crate) client_release: String,
    pub(crate) binary_sha256: String,
    pub(crate) successor_build: Option<String>,
    pub(crate) identities: Vec<serde_json::Value>,
    pub(crate) declared_changes: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ReleasePairReport {
    pub(crate) pair_id: String,
    pub(crate) baseline: BinaryIdentityRecord,
    pub(crate) successor: BinaryIdentityRecord,
    pub(crate) observed_protocol: ObservedProtocolState,
    pub(crate) cells: Vec<MatrixCellReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MatrixSummary {
    pub(crate) total_cells: usize,
    pub(crate) passed: usize,
    pub(crate) failed: usize,
    pub(crate) skipped: usize,
}

impl MatrixSummary {
    fn from_pairs(pairs: &[ReleasePairReport]) -> Self {
        let mut total_cells = 0;
        let mut passed = 0;
        let mut failed = 0;
        let mut skipped = 0;
        for pair in pairs {
            for cell in &pair.cells {
                total_cells += 1;
                match cell.status {
                    CellStatus::Passed => passed += 1,
                    CellStatus::Failed => failed += 1,
                    CellStatus::Skipped => skipped += 1,
                }
            }
        }
        Self {
            total_cells,
            passed,
            failed,
            skipped,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ForwardCompatMatrixReport {
    pub(crate) schema_version: u32,
    pub(crate) scenario: String,
    pub(crate) platform: String,
    pub(crate) generated_at: String,
    pub(crate) source_commit: Option<String>,
    pub(crate) source_dirty: Option<bool>,
    pub(crate) pairs: Vec<ReleasePairReport>,
    pub(crate) future_pairs_slot: String,
    pub(crate) summary: MatrixSummary,
    pub(crate) passed: bool,
}

fn current_platform_label() -> &'static str {
    if cfg!(target_os = "windows") {
        "windows-winfsp"
    } else if cfg!(target_os = "macos") {
        "macos-fuse"
    } else {
        "linux-fuse"
    }
}

fn write_matrix_report(
    path: &Path,
    report: &ForwardCompatMatrixReport,
) -> Result<(), ScenarioError> {
    let json = serde_json::to_string_pretty(report)?;
    if contains_unredacted_diagnostics(report) {
        return Err(io::Error::other("matrix report contains unredacted diagnostics").into());
    }
    fs::write(path, format!("{json}\n"))?;
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String, ScenarioError> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(hex_lower(&hasher.finalize()))
}

fn resolve_matrix_binaries(baseline_bin: &Path) -> Result<MatrixBinaries, ScenarioError> {
    if !baseline_bin.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "baseline studiofs binary not found: {}",
                baseline_bin.display()
            ),
        )
        .into());
    }

    let manifest_path = env::var_os(SUCCESSOR_MANIFEST_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_SUCCESSOR_MANIFEST));
    let (manifest_bin, source_commit, source_dirty) = if manifest_path.is_file() {
        let raw = fs::read_to_string(&manifest_path)?;
        let manifest: serde_json::Value = serde_json::from_str(&raw)?;
        if manifest
            .get("verified")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
        {
            return Err(io::Error::other(format!(
                "test-successor manifest at {} does not have verified=true; re-run tools/test_successor/build.py --require-complete",
                manifest_path.display()
            ))
            .into());
        }
        let bin = manifest
            .get("binary")
            .and_then(serde_json::Value::as_str)
            .map(PathBuf::from);
        let commit = manifest
            .get("source_commit")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned);
        let dirty = manifest
            .get("source_dirty")
            .and_then(serde_json::Value::as_bool);
        (bin, commit, dirty)
    } else {
        (None, None, None)
    };

    let successor_bin = env::var_os(SUCCESSOR_BIN_ENV)
        .map(PathBuf::from)
        .or(manifest_bin)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "compiled test-successor binary not configured; set {SUCCESSOR_BIN_ENV} or run `python tools/test_successor/build.py --require-complete` to produce {DEFAULT_SUCCESSOR_MANIFEST}"
                ),
            )
        })?;
    if !successor_bin.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "compiled test-successor binary not found at {}; run `python tools/test_successor/build.py --require-complete`",
                successor_bin.display()
            ),
        )
        .into());
    }

    let v024_bin = env::var_os(V024_BIN_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(default_cached_v024_bin);
    let require_v024 = env::var(REQUIRE_V024_ENV)
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes"))
        .unwrap_or(false);

    Ok(MatrixBinaries {
        baseline_bin: baseline_bin.to_path_buf(),
        successor_bin,
        v024_bin,
        require_v024,
        source_commit,
        source_dirty,
    })
}

fn default_cached_v024_bin() -> Option<PathBuf> {
    let exe = if cfg!(windows) {
        "studiofs.exe"
    } else {
        "studiofs"
    };
    let candidate = PathBuf::from("target/sfs-1002-published/v0.2.4/bin").join(exe);
    candidate.is_file().then_some(candidate)
}

fn query_version_identities(
    bin: &Path,
    label: &str,
) -> Result<BinaryIdentityRecord, ScenarioError> {
    let output = Command::new(bin)
        .args(["internal", "version-identities"])
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "`{} internal version-identities` failed (exit {:?}): {}",
            bin.display(),
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        ))
        .into());
    }
    let payload: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    Ok(BinaryIdentityRecord {
        label: label.to_string(),
        client_release: payload
            .get("client_release")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        binary_sha256: sha256_file(bin)?,
        successor_build: payload
            .get("successor_build")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        identities: payload
            .get("identities")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default(),
        declared_changes: payload
            .get("successor_changes")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default(),
    })
}

fn verify_successor_contract(
    baseline: &BinaryIdentityRecord,
    successor: &BinaryIdentityRecord,
) -> Result<(), ScenarioError> {
    if baseline.successor_build.is_some() {
        return Err(io::Error::other(
            "baseline binary carries test-successor marker; pass a clean baseline binary to --studiofs-bin",
        )
        .into());
    }
    if successor.successor_build.as_deref() != Some(EXPECTED_SUCCESSOR_MARKER) {
        return Err(io::Error::other(format!(
            "successor binary does not carry expected marker {EXPECTED_SUCCESSOR_MARKER}"
        ))
        .into());
    }
    if baseline.binary_sha256 == successor.binary_sha256 {
        return Err(
            io::Error::other("baseline and successor binaries have identical SHA-256").into(),
        );
    }
    for kind in REQUIRED_CHANGE_KINDS {
        let present = successor
            .declared_changes
            .iter()
            .any(|c| c.get("kind").and_then(serde_json::Value::as_str) == Some(kind));
        if !present {
            return Err(io::Error::other(format!(
                "test-successor binary is missing required declared change kind `{kind}`"
            ))
            .into());
        }
    }
    for rollback in REQUIRED_ROLLBACK_CLASSES {
        let present = successor
            .declared_changes
            .iter()
            .any(|c| c.get("rollback").and_then(serde_json::Value::as_str) == Some(rollback));
        if !present {
            return Err(io::Error::other(format!(
                "test-successor binary is missing required rollback class `{rollback}`"
            ))
            .into());
        }
    }
    Ok(())
}

fn run_cell<F>(
    config: &ClientControllerConfig,
    cells: &mut Vec<MatrixCellReport>,
    id: &str,
    category: &str,
    f: F,
) -> Result<(), ScenarioError>
where
    F: FnOnce(&mut Vec<OperationRecord>) -> Result<String, ScenarioError>,
{
    let started = Instant::now();
    let mut ops = Vec::new();
    let result = f(&mut ops);
    match result {
        Ok(detail) => {
            let status = if ops.iter().any(|op| op.status == CellStatus::Failed) {
                CellStatus::Failed
            } else {
                CellStatus::Passed
            };
            cells.push(MatrixCellReport {
                id: id.to_string(),
                category: category.to_string(),
                status: status.clone(),
                duration_ms: started.elapsed().as_millis() as u64,
                detail,
                operations: ops,
            });
            if status == CellStatus::Failed {
                return Err(io::Error::other(format!(
                    "matrix cell `{id}` recorded failed operation"
                ))
                .into());
            }
            Ok(())
        }
        Err(error) => {
            cells.push(MatrixCellReport {
                id: id.to_string(),
                category: category.to_string(),
                status: CellStatus::Failed,
                duration_ms: started.elapsed().as_millis() as u64,
                detail: matrix_error_detail(config, &error),
                operations: ops,
            });
            Err(error)
        }
    }
}

fn record_op(
    ops: &mut Vec<OperationRecord>,
    name: &str,
    status: CellStatus,
    detail: impl Into<String>,
) {
    ops.push(OperationRecord {
        name: name.to_string(),
        status,
        detail: detail.into(),
    });
}

fn assert_refusal(
    action: &str,
    output: &ClientCommandOutput,
    expected: &[&str],
) -> Result<(), ScenarioError> {
    let text = format!("{}\n{}", output.stdout, output.stderr);
    let normalized = text.to_ascii_lowercase();
    if output.status != 1 || !expected.iter().all(|reason| normalized.contains(reason)) {
        return Err(io::Error::other(format!(
            "{action}: expected CLI refusal with exit 1 and {expected:?}, got exit {}. See the retained probe logs",
            output.status
        ))
        .into());
    }
    Ok(())
}

fn mount_or_verify_refusal(
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
    name: &str,
    action: &str,
    expected: &[&str],
) -> Result<bool, ScenarioError> {
    let client = controller.client_by_name(name)?;
    let stdout = client.stdout_log.clone();
    let stderr = client.stderr_log.clone();
    let stdout_offset = log_len(&stdout)?;
    let stderr_offset = log_len(&stderr)?;

    match controller.mount_until_lifecycle_ready(name) {
        Ok(()) => Ok(true),
        Err(ClientControllerError::CommandFailed { status, .. }) => {
            // The controller reports the mount exit status. The child writes its
            // diagnostic to the retained logs, which also contain earlier commands.
            let output = ClientCommandOutput {
                status,
                stdout: read_log_suffix(&stdout, stdout_offset)?,
                stderr: read_log_suffix(&stderr, stderr_offset)?,
            };
            assert_refusal(action, &output, expected)?;
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

fn log_len(path: &Path) -> io::Result<u64> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error),
    }
}

fn read_log_suffix(path: &Path, offset: u64) -> io::Result<String> {
    let mut file = fs::File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

fn matrix_error_detail(config: &ClientControllerConfig, error: &ScenarioError) -> String {
    match error {
        ScenarioError::Client(ClientControllerError::CommandFailed {
            client,
            action,
            status,
            stdout,
            stderr,
        }) => format!(
            "client command {action} for {client} failed with exit status {status}: stderr: {}; stdout: {}",
            redact_matrix_output(config, stderr),
            redact_matrix_output(config, stdout),
        ),
        ScenarioError::CleanupAfterFailure {
            context,
            primary,
            cleanup,
        } => format!(
            "{context} failed: {}; cleanup failed: {}",
            matrix_error_detail(config, primary),
            matrix_error_detail(config, cleanup),
        ),
        _ => redact_matrix_output(config, &error.to_string()),
    }
}

fn redact_matrix_output(config: &ClientControllerConfig, text: &str) -> String {
    let mut redacted = text.to_string();
    for credential in [
        &config.meta_dsn,
        &config.s3.access_key,
        &config.s3.secret_key,
    ] {
        if let Ok(json) = serde_json::to_string(credential) {
            let escaped = &json[1..json.len() - 1];
            if escaped != credential {
                redacted = redacted.replace(escaped, "<redacted>");
            }
        }
    }
    redact_diagnostics(config, &redacted)
}

fn contains_unredacted_diagnostics(report: &ForwardCompatMatrixReport) -> bool {
    report
        .pairs
        .iter()
        .flat_map(|pair| &pair.cells)
        .any(|cell| {
            std::iter::once(cell.detail.as_str())
                .chain(cell.operations.iter().map(|op| op.detail.as_str()))
                .any(|detail| {
                    ["postgres://", "postgresql://"]
                        .iter()
                        .any(|token| detail.contains(token))
                })
        })
}

fn finish_cleanup<T>(
    context: &str,
    primary: Result<T, ScenarioError>,
    cleanup: Result<(), ScenarioError>,
) -> Result<T, ScenarioError> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => Err(ScenarioError::CleanupAfterFailure {
            context: context.to_string(),
            primary: Box::new(primary),
            cleanup: Box::new(cleanup),
        }),
    }
}

fn fast_sync_env() -> Vec<(String, String)> {
    vec![
        (
            "STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS".to_string(),
            "250".to_string(),
        ),
        ("STUDIOFS_PG_SYNC_POLL_SECS".to_string(), "1".to_string()),
        (
            "STUDIOFS_PG_CHECKPOINT_MAX_VERSION_LAG".to_string(),
            "1".to_string(),
        ),
        (
            "STUDIOFS_PG_CHECKPOINT_RETRY_MS".to_string(),
            "200".to_string(),
        ),
        (
            "STUDIOFS_PG_EVENT_LOG_PRUNE_BATCHES".to_string(),
            "1".to_string(),
        ),
    ]
}

fn run_baseline_successor_pair(
    context: &ScenarioContext,
    config: &ClientControllerConfig,
    compose: &ComposeServiceControl,
    binaries: &MatrixBinaries,
    pair: &mut ReleasePairReport,
) -> Result<(), ScenarioError> {
    let mut controller = MultiClientController::new(config.clone(), SystemClientProcessExecutor);
    controller.create_clients([
        CLIENT_SUCCESSOR,
        CLIENT_BASELINE,
        CLIENT_COLD_BASELINE,
        CLIENT_COLD_SUCCESSOR,
        CLIENT_BASELINE_PROBE,
        "client-post-activation",
    ])?;

    controller.set_client_bin_override("client-post-activation", binaries.successor_bin.clone())?;
    controller.set_client_bin_override(CLIENT_SUCCESSOR, binaries.successor_bin.clone())?;
    controller.set_client_bin_override(CLIENT_COLD_SUCCESSOR, binaries.successor_bin.clone())?;
    for name in [
        CLIENT_SUCCESSOR,
        CLIENT_BASELINE,
        CLIENT_COLD_BASELINE,
        CLIENT_COLD_SUCCESSOR,
    ] {
        controller.set_client_env_overrides(name, fast_sync_env())?;
    }

    let result = (|| -> Result<(), ScenarioError> {
        // Cell 1: Additive PG migration (core=2) + baseline DDL refusal + concurrent two-way RW + chunk/slice + xattrs.
        run_cell(
            config,
            &mut pair.cells,
            "cell_01_additive_pg_migration_and_concurrent_rw",
            "supported_concurrent_rw",
            |ops| {
                if crash_rollback_enabled() {
                    exercise_migration_restart(&mut controller, config, binaries, ops)?;
                } else {
                    // Successor initializes the volume with the additive core=2 migration.
                    controller.init_first_client()?;
                }
                record_op(
                    ops,
                    "successor_init_core_schema_v2",
                    CellStatus::Passed,
                    "compiled test-successor initialized volume and applied core schema v2",
                );

                let proto = observe_protocol_state(config)?;
                let core_ver = proto
                    .schema_components
                    .iter()
                    .find(|c| c.component == "core")
                    .map(|c| c.version);
                if core_ver != Some(2) {
                    return Err(io::Error::other(format!(
                        "expected core schema version 2 after successor init, got {core_ver:?}"
                    ))
                    .into());
                }
                record_op(
                    ops,
                    "verify_pg_core_schema_v2",
                    CellStatus::Passed,
                    format!(
                        "metadata_format_version={:?}, core=2, components={}",
                        proto.metadata_format_version,
                        proto.schema_components.len()
                    ),
                );

                // Baseline must refuse DDL (`init` on a second volume) once core=2 is recorded.
                let ddl_out = controller.run_cli_allow_failure(
                    CLIENT_BASELINE_PROBE,
                    "baseline-init-refused-on-core-v2",
                    vec![
                        "init".to_string(),
                        format!("{}-blocked", config.volume_name),
                        "--endpoint".to_string(),
                        config.s3.endpoint.clone(),
                        "--bucket".to_string(),
                        config.s3.bucket.clone(),
                        "--access-key".to_string(),
                        config.s3.access_key.clone(),
                        "--secret-key".to_string(),
                        config.s3.secret_key.clone(),
                        "--region".to_string(),
                        config.s3.region.clone(),
                        "--meta".to_string(),
                        config.meta_dsn.clone(),
                    ],
                )?;
                assert_refusal(
                    "baseline init against core schema v2",
                    &ddl_out,
                    &[
                        "upgrade required",
                        "schema component `core` is at version 2, newer than 1",
                    ],
                )?;
                record_op(
                    ops,
                    "baseline_ddl_refused_on_newer_schema",
                    CellStatus::Passed,
                    format!("0.2.5 baseline `init` refused with exit {}", ddl_out.status),
                );

                // Baseline discovers and mounts concurrently alongside successor.
                controller.discover(CLIENT_BASELINE)?;
                controller.mount(CLIENT_SUCCESSOR)?;
                controller.mount(CLIENT_BASELINE)?;
                if crash_rollback_enabled() {
                    let mount = &controller.client_by_name(CLIENT_BASELINE)?.mount_point;
                    wait_for_matrix_state(
                        "pre-migration payload on baseline",
                        CONVERGENCE_TIMEOUT,
                        |deadline| match bounded_mounted_read(
                            mount.join("before-migration.txt"),
                            "read recovered pre-migration payload".to_string(),
                            remaining_observation_time(deadline),
                        ) {
                            Ok(actual) => {
                                Ok(recovered_payload_matches(&actual, PRE_MIGRATION_PAYLOAD))
                            }
                            Err(ScenarioError::Io(error))
                                if error.kind() == io::ErrorKind::NotFound =>
                            {
                                Ok(false)
                            }
                            Err(error) => Err(error),
                        },
                    )?;
                    record_op(
                        ops,
                        "pre_migration_payload_checksum",
                        CellStatus::Passed,
                        "a fresh baseline client read the original payload after interrupted migration and retry",
                    );
                }

                let succ_mount = controller
                    .client_by_name(CLIENT_SUCCESSOR)?
                    .mount_point
                    .clone();
                let base_mount = controller
                    .client_by_name(CLIENT_BASELINE)?
                    .mount_point
                    .clone();

                // Two-way create / read / write / overwrite / rename / delete.
                fs::create_dir_all(succ_mount.join("compat-dir"))?;
                write_sync(
                    &succ_mount.join("compat-dir/from-successor.txt"),
                    b"successor-initial-payload\n",
                )?;
                write_sync(
                    &base_mount.join("from-baseline.txt"),
                    b"baseline-initial-payload\n",
                )?;
                wait_pair_converged(context, &succ_mount, &base_mount, "cell01-initial")?;
                record_op(
                    ops,
                    "two_way_create_write_read",
                    CellStatus::Passed,
                    "baseline and successor converged on two-way creates and reads",
                );

                // Cross-binary overwrite, rename, and delete.
                write_sync(
                    &base_mount.join("compat-dir/from-successor.txt"),
                    b"overwritten-by-baseline\n",
                )?;
                fs::rename(
                    succ_mount.join("from-baseline.txt"),
                    succ_mount.join("compat-dir/from-baseline-renamed.txt"),
                )?;
                write_sync(&base_mount.join("ephemeral.txt"), b"delete-me\n")?;
                wait_pair_converged(
                    context,
                    &succ_mount,
                    &base_mount,
                    "cell01-rename-pre-delete",
                )?;
                fs::remove_file(succ_mount.join("ephemeral.txt"))?;
                wait_pair_converged(context, &succ_mount, &base_mount, "cell01-rename-delete")?;
                if base_mount.join("ephemeral.txt").exists()
                    || base_mount.join("from-baseline.txt").exists()
                {
                    return Err(io::Error::other(
                        "deleted or renamed source path still visible on baseline mount",
                    )
                    .into());
                }
                record_op(
                    ops,
                    "two_way_overwrite_rename_delete",
                    CellStatus::Passed,
                    "cross-binary overwrite, rename, and delete converged cleanly",
                );

                // Multi-chunk / slice replay across chunk boundary.
                let mut chunk_payload = deterministic_bytes(MULTI_CHUNK_BYTES, 0x5A);
                let chunk_path_base = base_mount.join("compat-dir/multi-chunk.bin");
                let chunk_path_succ = succ_mount.join("compat-dir/multi-chunk.bin");
                write_sync(&chunk_path_base, &chunk_payload)?;
                wait_pair_converged(
                    context,
                    &succ_mount,
                    &base_mount,
                    "cell01-multichunk-create",
                )?;

                // Partial overwrite spanning the 4 MiB chunk boundary from the successor binary.
                let patch_offset = (DEFAULT_CHUNK_SIZE as usize).saturating_sub(4096);
                let patch = deterministic_bytes(16 * 1024, 0xC3);
                {
                    let mut file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(&chunk_path_succ)?;
                    file.seek(SeekFrom::Start(patch_offset as u64))?;
                    file.write_all(&patch)?;
                    file.sync_all()?;
                }
                chunk_payload[patch_offset..patch_offset + patch.len()].copy_from_slice(&patch);
                wait_pair_converged(context, &succ_mount, &base_mount, "cell01-multichunk-patch")?;
                let readback_base = fs::read(&chunk_path_base)?;
                if readback_base != chunk_payload {
                    return Err(io::Error::other(
                        "baseline readback mismatch after successor cross-chunk slice overwrite",
                    )
                    .into());
                }
                record_op(
                    ops,
                    "chunk_slice_replay_cross_boundary",
                    CellStatus::Passed,
                    format!(
                        "verified {MULTI_CHUNK_BYTES}-byte multi-chunk write + 16 KiB cross-boundary slice overwrite"
                    ),
                );

                // Xattrs: native user.* xattr convergence on Unix; explicit platform-gated note on Windows.
                let xattr_op = exercise_mounted_xattr(
                    &succ_mount.join("compat-dir/from-successor.txt"),
                    &base_mount.join("compat-dir/from-successor.txt"),
                )?;
                ops.push(xattr_op);

                Ok("additive PG migration (core=2), baseline DDL refusal, concurrent two-way RW, and multi-chunk slice replay passed".to_string())
            },
        )?;

        // Cell 2: Dual serving checkpoint refresh (format 4 + format 5) & event log pruning + cold discover.
        run_cell(
            config,
            &mut pair.cells,
            "cell_02_dual_checkpoint_refresh_and_event_pruning",
            "checkpoint_and_pruning",
            |ops| {
                let succ_mount = controller
                    .client_by_name(CLIENT_SUCCESSOR)?
                    .mount_point
                    .clone();
                let base_mount = controller
                    .client_by_name(CLIENT_BASELINE)?
                    .mount_point
                    .clone();

                let before = if crash_rollback_enabled() {
                    matrix_sql(
                        config,
                        "install checkpoint and prune cancellation",
                        INSTALL_CHECKPOINT_AND_PRUNE_FAULTS,
                    )?;
                    Some(observe_protocol_state(config)?)
                } else {
                    None
                };
                // Generate a few batches so checkpoint coordinator publishes format 4 + format 5 and prunes.
                for idx in 0..4 {
                    let path = succ_mount.join(format!("compat-dir/ckpt-batch-{idx}.txt"));
                    write_sync(&path, format!("checkpoint-batch-{idx}\n").as_bytes())?;
                }
                wait_pair_converged(context, &succ_mount, &base_mount, "cell02-pre-checkpoint")?;

                if let Some(before) = &before {
                    wait_for_cancelled_transaction(
                        config,
                        "checkpoint publication cancellation",
                        "SELECT is_called FROM sfs1020_checkpoint_attempt",
                    )?;
                    let cancelled = observe_protocol_state(config)?;
                    if before.checkpoint_version != cancelled.checkpoint_version
                        || before.checkpoint_formats_observed
                            != cancelled.checkpoint_formats_observed
                    {
                        return Err(io::Error::other(
                            "cancelled checkpoint replaced persisted checkpoint state",
                        )
                        .into());
                    }
                    controller.force_kill(CLIENT_SUCCESSOR)?;
                    matrix_sql(
                        config,
                        "remove checkpoint cancellation",
                        "DROP TRIGGER sfs1020_cancel_checkpoint ON metadata_state",
                    )?;
                    controller.remount(CLIENT_SUCCESSOR)?;
                    record_op(
                        ops,
                        "checkpoint_cancellation_and_restart",
                        CellStatus::Passed,
                        "a cancelled checkpoint transaction preserved the prior checkpoint and formats; the successor remounted",
                    );

                    wait_for_cancelled_transaction(
                        config,
                        "event-log pruning cancellation",
                        "SELECT is_called FROM sfs1020_prune_attempt",
                    )?;
                    let cancelled = observe_protocol_state(config)?;
                    if before.event_log_prune_floor != cancelled.event_log_prune_floor {
                        return Err(
                            io::Error::other("cancelled pruning advanced the prune floor").into(),
                        );
                    }
                    matrix_sql(
                        config,
                        "remove prune cancellation",
                        "DROP TRIGGER sfs1020_cancel_prune ON metadata_state;
                         DROP FUNCTION sfs1020_cancel_maintenance();
                         DROP SEQUENCE sfs1020_checkpoint_attempt, sfs1020_prune_attempt;",
                    )?;
                    record_op(
                        ops,
                        "prune_floor_preserved_after_cancellation",
                        CellStatus::Passed,
                        "prune transaction was cancelled before commit and left the previous floor unchanged",
                    );
                }
                let proto = wait_for_dual_checkpoint_and_prune(
                    config,
                    CONVERGENCE_TIMEOUT,
                    before.as_ref(),
                )?;
                record_op(
                    ops,
                    "dual_checkpoint_format_4_and_5",
                    CellStatus::Passed,
                    format!(
                        "checkpoint_version={:?}, formats={:?}",
                        proto.checkpoint_version, proto.checkpoint_formats_observed
                    ),
                );
                record_op(
                    ops,
                    "event_log_pruning",
                    CellStatus::Passed,
                    format!("event_log_prune_floor={:?}", proto.event_log_prune_floor),
                );

                // Cold baseline client (bootstraps format 4) & cold successor client (bootstraps format 5).
                controller.discover(CLIENT_COLD_BASELINE)?;
                controller.discover(CLIENT_COLD_SUCCESSOR)?;
                controller.mount(CLIENT_COLD_BASELINE)?;
                controller.mount(CLIENT_COLD_SUCCESSOR)?;

                let cold_base_mount = controller
                    .client_by_name(CLIENT_COLD_BASELINE)?
                    .mount_point
                    .clone();
                let cold_succ_mount = controller
                    .client_by_name(CLIENT_COLD_SUCCESSOR)?
                    .mount_point
                    .clone();
                let mounts = vec![
                    ClientMount::new(CLIENT_SUCCESSOR, succ_mount),
                    ClientMount::new(CLIENT_BASELINE, base_mount),
                    ClientMount::new(CLIENT_COLD_BASELINE, cold_base_mount),
                    ClientMount::new(CLIENT_COLD_SUCCESSOR, cold_succ_mount),
                ];
                crate::mounted_tree::wait_for_convergence_with_options(
                    &mounts,
                    context.scenario_dir.join("cell02-cold-bootstrap"),
                    CONVERGENCE_TIMEOUT,
                    POLL_INTERVAL,
                    // Cold clients fetch and hash the full multi-chunk file from RustFS.
                    Duration::from_secs(60),
                    &Default::default(),
                )?;
                controller.unmount(CLIENT_COLD_BASELINE)?;
                controller.unmount(CLIENT_COLD_SUCCESSOR)?;
                record_op(
                    ops,
                    "cold_bootstrap_after_checkpoint_and_prune",
                    CellStatus::Passed,
                    "fresh baseline (format 4) and successor (format 5) clients bootstrapped and converged after pruning",
                );

                pair.observed_protocol = proto;
                Ok("dual checkpoint refresh (format 4 + 5), event pruning, and cold bootstrap convergence passed".to_string())
            },
        )?;

        // Cell 3: Cross-binary Snapshot v2 create + read-only smount verification.
        run_cell(
            config,
            &mut pair.cells,
            "cell_03_snapshots_cross_binary",
            "snapshots",
            |ops| {
                let succ_mount = controller
                    .client_by_name(CLIENT_SUCCESSOR)?
                    .mount_point
                    .clone();
                let base_mount = controller
                    .client_by_name(CLIENT_BASELINE)?
                    .mount_point
                    .clone();
                let pre_cut_bytes = b"snapshot-pre-cut-content-sfs-1019\n";
                write_sync(
                    &base_mount.join("compat-dir/snap-target.txt"),
                    pre_cut_bytes,
                )?;
                wait_pair_converged(context, &succ_mount, &base_mount, "cell03-pre-snapshot")?;

                let create_args: Vec<String> = snapshot_create_args(&config.volume_name)
                    .into_iter()
                    .map(ToOwned::to_owned)
                    .collect();
                let create_out =
                    controller.run_cli(CLIENT_BASELINE, "snapshot-create", create_args)?;
                let snap_id = parse_snapshot_id(&create_out.stdout).ok_or_else(|| {
                    io::Error::other(format!(
                        "could not parse snapshot id from output: {}",
                        create_out.stdout.trim()
                    ))
                })?;
                wait_for_snapshot_ready(config, &snap_id, SNAPSHOT_TIMEOUT)?;
                record_op(
                    ops,
                    "snapshot_create_ready",
                    CellStatus::Passed,
                    format!("snapshot {snap_id} reached ready state"),
                );

                // Post-cut mutation via successor must NOT appear in snapshot mount.
                write_sync(
                    &succ_mount.join("compat-dir/post-cut-only.txt"),
                    b"after-cut\n",
                )?;
                write_sync(
                    &succ_mount.join("compat-dir/snap-target.txt"),
                    b"mutated-after-cut\n",
                )?;
                wait_pair_converged(context, &succ_mount, &base_mount, "cell03-post-snapshot")?;

                // Mount snapshot read-only with the successor binary.
                let snap_client = controller.client_by_name(CLIENT_COLD_SUCCESSOR)?.clone();
                let snap_mount = available_snapshot_mount_point(&context.sandbox)
                    .map(|mount| crate::client::product_mount_path(&config.volume_name, mount))
                    .ok_or_else(|| io::Error::other("no available snapshot mount point"))?;
                let mut snap_proc = spawn_snapshot_mount(
                    &binaries.successor_bin,
                    &snap_client,
                    &config.volume_name,
                    &snap_id,
                    &snap_mount,
                    &context.scenario_dir,
                )?;
                let snap_verify = (|| -> Result<(), ScenarioError> {
                    wait_for_mount_active(&snap_mount, Duration::from_secs(45))?;
                    let got = fs::read(snap_mount.join("compat-dir/snap-target.txt"))?;
                    if got != pre_cut_bytes {
                        return Err(io::Error::other(
                            "snapshot mount readback did not match pre-cut bytes",
                        )
                        .into());
                    }
                    if snap_mount.join("compat-dir/post-cut-only.txt").exists() {
                        return Err(io::Error::other(
                            "post-cut file unexpectedly visible inside snapshot mount",
                        )
                        .into());
                    }
                    Ok(())
                })();
                let snap_unmount = unmount_snapshot_mount(
                    &binaries.successor_bin,
                    &snap_client,
                    &snap_mount,
                    &mut snap_proc,
                );
                finish_cleanup("snapshot readback", snap_verify, snap_unmount)?;
                record_op(
                    ops,
                    "snapshot_smount_cross_binary_readback",
                    CellStatus::Passed,
                    "successor smount verified pre-cut bytes and excluded post-cut writes",
                );

                Ok(format!(
                    "cross-binary Snapshot v2 ({snap_id}) create and read-only mount verified"
                ))
            },
        )?;

        // Cell 4: Reconnect after PostgreSQL outage with pending local baseline write.
        run_cell(
            config,
            &mut pair.cells,
            "cell_04_reconnect_and_offline_pending_work",
            "reconnect",
            |ops| {
                let succ_mount = controller
                    .client_by_name(CLIENT_SUCCESSOR)?
                    .mount_point
                    .clone();
                let base_mount = controller
                    .client_by_name(CLIENT_BASELINE)?
                    .mount_point
                    .clone();

                let executor = SystemCommandExecutor;
                compose.stop_service(POSTGRES_SERVICE_NAME, &executor)?;
                let offline_bytes = b"accepted-while-postgres-down-sfs-1019\n";
                let crash_rollback = crash_rollback_enabled();
                let outage_result = (|| -> Result<(), ScenarioError> {
                    let path = base_mount.join("compat-dir/offline-baseline.txt");
                    write_sync(&path, offline_bytes)?;
                    let local_read =
                        bounded_mounted_read(path, "offline local readback", CONVERGENCE_TIMEOUT)?;
                    if !recovered_payload_matches(&local_read, offline_bytes) {
                        return Err(io::Error::other(
                            "offline local readback mismatch on baseline",
                        )
                        .into());
                    }
                    record_op(
                        ops,
                        "offline_local_write_accepted",
                        CellStatus::Passed,
                        "baseline accepted and read back local write while PostgreSQL was stopped",
                    );
                    if crash_rollback {
                        controller.force_kill(CLIENT_BASELINE)?;
                    }
                    Ok(())
                })();
                let restore = compose
                    .start_service(POSTGRES_SERVICE_NAME, &executor)
                    .map_err(Into::into);
                finish_cleanup("PostgreSQL outage", outage_result, restore)?;

                if crash_rollback {
                    controller.remount(CLIENT_BASELINE)?;
                    let recovered = bounded_mounted_read(
                        base_mount.join("compat-dir/offline-baseline.txt"),
                        "read recovered unpublished payload".to_string(),
                        CONVERGENCE_TIMEOUT,
                    )?;
                    if !recovered_payload_matches(&recovered, offline_bytes) {
                        return Err(io::Error::other(
                            "baseline lost the unpublished payload after restart",
                        )
                        .into());
                    }
                    record_op(
                        ops,
                        "restart_with_unpublished_write",
                        CellStatus::Passed,
                        "baseline remounted its original cache after force-kill and recovered the locally synced checksum",
                    );
                }
                wait_pair_converged(context, &succ_mount, &base_mount, "cell04-post-reconnect")?;
                let peer_read = fs::read(succ_mount.join("compat-dir/offline-baseline.txt"))?;
                if peer_read != offline_bytes {
                    return Err(io::Error::other(
                        "successor readback mismatch after baseline reconnected",
                    )
                    .into());
                }
                record_op(
                    ops,
                    "reconnect_publish_and_converge",
                    CellStatus::Passed,
                    "baseline reconnected after PostgreSQL restart and published pending write to successor",
                );

                Ok(
                    "PostgreSQL outage reconnect and pending local write publication passed"
                        .to_string(),
                )
            },
        )?;

        // Cell 5: Session expiry and SFS-1017 feature cutover / activation refusal + reversible rollback.
        run_cell(
            config,
            &mut pair.cells,
            "cell_05_session_expiry_and_activation_refusal",
            "activation_and_sessions",
            |ops| {
                // Ensure both active sessions are registered in client_sessions before cutover.
                wait_for_active_sessions(config, 2, Duration::from_secs(30))?;

                // Unmount successor temporarily to drive feature cutover (cutover requires local cache unmounted).
                controller.unmount(CLIENT_SUCCESSOR)?;

                // 1. Expand (`available`)
                controller.run_cli(
                    CLIENT_SUCCESSOR,
                    "cutover-expand",
                    vec![
                        "upgrade".into(),
                        config.volume_name.clone(),
                        "--feature".into(),
                        SUCCESSOR_XATTR_CAPABILITY.into(),
                        "--phase".into(),
                        "expand".into(),
                    ],
                )?;
                record_op(
                    ops,
                    "cutover_expand_available",
                    CellStatus::Passed,
                    format!("expanded {SUCCESSOR_XATTR_CAPABILITY} to available"),
                );

                // 2. Migrate (`compatible`)
                controller.run_cli(
                    CLIENT_SUCCESSOR,
                    "cutover-migrate",
                    vec![
                        "upgrade".into(),
                        config.volume_name.clone(),
                        "--feature".into(),
                        SUCCESSOR_XATTR_CAPABILITY.into(),
                        "--phase".into(),
                        "migrate".into(),
                    ],
                )?;
                record_op(
                    ops,
                    "cutover_migrate_compatible",
                    CellStatus::Passed,
                    format!("migrated {SUCCESSOR_XATTR_CAPABILITY} to compatible"),
                );

                // 3. Activate while baseline session is ACTIVE -> MUST BE REFUSED.
                let baseline_sessions = active_baseline_sessions(config)?;
                let epoch_before = observe_protocol_state(config)?.policy_epoch;
                let active_refusal = controller.run_cli_allow_failure(
                    CLIENT_SUCCESSOR,
                    "cutover-activate-refused-active-baseline",
                    vec![
                        "upgrade".into(),
                        config.volume_name.clone(),
                        "--feature".into(),
                        SUCCESSOR_XATTR_CAPABILITY.into(),
                        "--phase".into(),
                        "activate".into(),
                        "--role".into(),
                        "read".into(),
                    ],
                )?;
                assert_refusal(
                    "activation with an active baseline session",
                    &active_refusal,
                    &["active session ", SUCCESSOR_XATTR_CAPABILITY],
                )?;
                if !baseline_sessions.iter().any(|id| {
                    active_refusal
                        .stderr
                        .contains(&format!("active session {id}"))
                }) {
                    return Err(io::Error::other(
                        "cutover refusal did not identify an observed baseline session",
                    )
                    .into());
                }
                if observe_protocol_state(config)?.policy_epoch != epoch_before {
                    return Err(io::Error::other(
                        "refused activation unexpectedly mutated policy_epoch",
                    )
                    .into());
                }
                record_op(
                    ops,
                    "activate_refused_with_active_baseline_session",
                    CellStatus::Passed,
                    format!(
                        "activation refused (exit {}) while baseline session active; policy_epoch unchanged",
                        active_refusal.status
                    ),
                );

                // 4. Crash/stop baseline and expire its session lease in PG to test expired-session refusal.
                controller.force_kill(CLIENT_BASELINE)?;
                expire_all_client_sessions(config)?;

                let epoch_before = observe_protocol_state(config)?.policy_epoch;
                let expired_refusal = controller.run_cli_allow_failure(
                    CLIENT_SUCCESSOR,
                    "cutover-activate-refused-expired-session",
                    vec![
                        "upgrade".into(),
                        config.volume_name.clone(),
                        "--feature".into(),
                        SUCCESSOR_XATTR_CAPABILITY.into(),
                        "--phase".into(),
                        "activate".into(),
                        "--role".into(),
                        "read".into(),
                    ],
                )?;
                assert_refusal(
                    "activation with an expired baseline session",
                    &expired_refusal,
                    &[
                        "expired session ",
                        SUCCESSOR_XATTR_CAPABILITY,
                        "--accept-offline-clients",
                    ],
                )?;
                if observe_protocol_state(config)?.policy_epoch != epoch_before {
                    return Err(io::Error::other("refused activation changed policy_epoch").into());
                }
                record_op(
                    ops,
                    "activate_refused_with_expired_session_without_override",
                    CellStatus::Passed,
                    format!(
                        "activation refused (exit {}) for expired unsupported session without --accept-offline-clients",
                        expired_refusal.status
                    ),
                );

                // 5. Reversible rollback: `deactivate` (`compatible -> available`), then prove baseline still mounts & writes.
                controller.run_cli(
                    CLIENT_SUCCESSOR,
                    "cutover-deactivate-rollback",
                    vec![
                        "upgrade".into(),
                        config.volume_name.clone(),
                        "--feature".into(),
                        SUCCESSOR_XATTR_CAPABILITY.into(),
                        "--phase".into(),
                        "deactivate".into(),
                    ],
                )?;
                if observe_protocol_state(config)?.policy_epoch <= epoch_before {
                    return Err(
                        io::Error::other("deactivate did not advance the policy epoch").into(),
                    );
                }
                controller.remount(CLIENT_BASELINE)?;
                let base_mount = controller
                    .client_by_name(CLIENT_BASELINE)?
                    .mount_point
                    .clone();
                let before_rollback_write =
                    observe_metadata_version(config, Duration::from_secs(15))?;
                write_sync(
                    &base_mount.join("compat-dir/post-deactivate.txt"),
                    b"reversible-rollback-ok\n",
                )?;
                wait_for_published_write(
                    &controller,
                    CLIENT_BASELINE,
                    config,
                    before_rollback_write,
                )?;
                // Cutover checks the stopped successor's local replay position.
                // Catch it up to the baseline write before using it to activate.
                controller.mount(CLIENT_SUCCESSOR)?;
                let successor_mount = &controller.client_by_name(CLIENT_SUCCESSOR)?.mount_point;
                wait_pair_converged(
                    context,
                    successor_mount,
                    &base_mount,
                    "cell05-after-deactivate",
                )?;
                controller.unmount(CLIENT_BASELINE)?;
                controller.unmount(CLIENT_SUCCESSOR)?;
                record_op(
                    ops,
                    "reversible_deactivate_keeps_baseline_rw",
                    CellStatus::Passed,
                    "deactivated compatible -> available and verified baseline remount + write",
                );

                // 6. Re-migrate and activate with `--role read --accept-offline-clients`.
                controller.run_cli(
                    CLIENT_SUCCESSOR,
                    "cutover-remigrate",
                    vec![
                        "upgrade".into(),
                        config.volume_name.clone(),
                        "--feature".into(),
                        SUCCESSOR_XATTR_CAPABILITY.into(),
                        "--phase".into(),
                        "migrate".into(),
                    ],
                )?;
                expire_all_client_sessions(config)?;
                let before_activation = observe_shared_state(config)?;
                let activate = vec![
                    "upgrade".into(),
                    config.volume_name.clone(),
                    "--feature".into(),
                    SUCCESSOR_XATTR_CAPABILITY.into(),
                    "--phase".into(),
                    "activate".into(),
                    "--role".into(),
                    "read".into(),
                    "--accept-offline-clients".into(),
                ];
                if crash_rollback_enabled() {
                    matrix_sql(config, "install cutover cancellation",
                        "CREATE FUNCTION sfs1020_cancel_activation() RETURNS trigger LANGUAGE plpgsql AS $$
                         BEGIN
                             RAISE EXCEPTION USING ERRCODE = '57014', MESSAGE = 'SFS-1020 cutover cancellation';
                         END $$;
                         CREATE TRIGGER sfs1020_cancel_activation AFTER UPDATE OF policy_epoch ON volume_protocol
                         FOR EACH ROW EXECUTE FUNCTION sfs1020_cancel_activation();")?;
                    let interrupted = controller.run_cli_allow_failure(
                        CLIENT_SUCCESSOR,
                        "cancel-cutover-activation",
                        activate.clone(),
                    );
                    matrix_sql(
                        config,
                        "remove cutover cancellation",
                        "DROP TRIGGER sfs1020_cancel_activation ON volume_protocol;
                         DROP FUNCTION sfs1020_cancel_activation();",
                    )?;
                    assert_refusal("cancelled feature activation", &interrupted?, &["57014"])?;
                    assert_shared_state_unchanged(
                        config,
                        &before_activation,
                        "cancelled feature activation",
                    )?;
                    record_op(
                        ops,
                        "activation_cancellation_and_retry",
                        CellStatus::Passed,
                        "cancelled feature activation preserved the policy epoch and event state",
                    );
                }
                controller.run_cli(
                    CLIENT_SUCCESSOR,
                    "cutover-activate-accept-offline",
                    activate,
                )?;
                let post_activate = observe_protocol_state(config)?;
                let activated = observe_shared_state(config)?;
                if activated.policy_epoch <= before_activation.policy_epoch
                    || activated.publication_epoch <= before_activation.publication_epoch
                    || activated.policy_epoch != Some(activated.publication_epoch)
                {
                    return Err(
                        io::Error::other("activation did not advance the policy epoch").into(),
                    );
                }
                record_op(
                    ops,
                    "activate_with_accept_offline_clients",
                    CellStatus::Passed,
                    format!(
                        "activated {SUCCESSOR_XATTR_CAPABILITY} (read); policy_epoch={:?}",
                        post_activate.policy_epoch
                    ),
                );
                pair.observed_protocol = post_activate;

                Ok("session expiry, activation refusal, reversible deactivate rollback, and explicit activation passed".to_string())
            },
        )?;

        // Cell 6: Unsupported read-incompatible activation fails baseline closed without corrupting PG/S3/local redb.
        run_cell(
            config,
            &mut pair.cells,
            "cell_06_unsupported_activation_fail_closed",
            "unsupported_fail_closed",
            |ops| {
                let baseline_db = controller
                    .client_by_name(CLIENT_BASELINE)?
                    .local_metadata_db
                    .clone();
                let sentinel = Path::new("compat-dir/offline-baseline.txt");
                let sentinel_bytes = b"accepted-while-postgres-down-sfs-1019\n";
                let baseline_before =
                    offline_sentinel(&baseline_db, sentinel, sentinel_bytes.len())?;
                let before_discover = observe_shared_state(config)?;

                // 1. `studiofs compatibility` on baseline must report upgrade_required.
                let compat_out = controller.run_cli_allow_failure(
                    CLIENT_BASELINE,
                    "baseline-compatibility-after-activate",
                    vec!["compatibility".into(), config.volume_name.clone()],
                )?;
                if !compat_out
                    .stdout
                    .lines()
                    .any(|line| line.trim() == "Overall: upgrade_required")
                {
                    return Err(io::Error::other(
                        "baseline compatibility report did not report upgrade_required",
                    )
                    .into());
                }
                record_op(
                    ops,
                    "baseline_compatibility_reports_upgrade_required",
                    CellStatus::Passed,
                    "baseline `studiofs compatibility` reported upgrade required",
                );

                // 2. Fresh baseline `discover` must fail closed immediately.
                let discover_fail = controller.run_cli_allow_failure(
                    CLIENT_BASELINE_PROBE,
                    "baseline-discover-refused-after-activate",
                    vec![
                        "discover".into(),
                        config.volume_name.clone(),
                        "--meta".into(),
                        config.meta_dsn.clone(),
                    ],
                )?;
                let feature_refusal = [
                    "upgrade required",
                    "event_kind_unknown",
                    SUCCESSOR_XATTR_CAPABILITY,
                ];
                assert_refusal(
                    "baseline discovery after activation",
                    &discover_fail,
                    &feature_refusal,
                )?;
                assert_shared_state_unchanged(config, &before_discover, "baseline discovery")?;
                record_op(
                    ops,
                    "baseline_discover_refused_after_activate",
                    CellStatus::Passed,
                    format!(
                        "fresh baseline `discover` failed closed with exit {}",
                        discover_fail.status
                    ),
                );

                // 3. Successor mounts, reads prior data, and writes a post-activation file.
                controller.mount(CLIENT_SUCCESSOR)?;
                let succ_mount = controller
                    .client_by_name(CLIENT_SUCCESSOR)?
                    .mount_point
                    .clone();
                let readback = fs::read(succ_mount.join("compat-dir/offline-baseline.txt"))?;
                if readback != b"accepted-while-postgres-down-sfs-1019\n" {
                    return Err(io::Error::other(
                        "successor readback mismatch after incompatible activation",
                    )
                    .into());
                }
                let post_activate_path = "compat-dir/post-activate-successor.txt";
                let post_activate_bytes = b"successor-post-activate\n";
                write_sync(&succ_mount.join(post_activate_path), post_activate_bytes)?;
                wait_for_published_write(
                    &controller,
                    CLIENT_SUCCESSOR,
                    config,
                    before_discover.version,
                )?;
                controller.unmount(CLIENT_SUCCESSOR)?;
                let before_mount = observe_shared_state(config)?;

                // 4. Existing baseline local cache either fails closed at mount startup OR enters
                //    local-first degraded/fenced mode (stopping replay and blocking publication).
                match mount_or_verify_refusal(
                    &mut controller,
                    CLIENT_BASELINE,
                    "baseline mount after activation",
                    &feature_refusal,
                )? {
                    false => {
                        record_op(
                            ops,
                            "baseline_mount_fenced_after_activate",
                            CellStatus::Passed,
                            "baseline mount refused startup after read-incompatible activation",
                        );
                    }
                    true => {
                        let base_mount = controller
                            .client_by_name(CLIENT_BASELINE)?
                            .mount_point
                            .clone();
                        wait_for_runtime_fence(&controller, CLIENT_BASELINE)?;
                        assert_missing_file(&base_mount.join(post_activate_path))?;
                        controller.unmount(CLIENT_BASELINE)?;
                        record_op(
                            ops,
                            "baseline_mount_fenced_after_activate",
                            CellStatus::Passed,
                            "runtime entered degraded/fenced sync state after successor publication",
                        );
                    }
                }

                if offline_sentinel(&baseline_db, sentinel, sentinel_bytes.len())?
                    != baseline_before
                {
                    return Err(io::Error::other(
                        "baseline sentinel inode or payload map changed after refusal",
                    )
                    .into());
                }
                record_op(
                    ops,
                    "baseline_local_redb_preserved",
                    CellStatus::Passed,
                    "offline read-only redb inspection verified the sentinel inode, size, and payload map",
                );

                assert_shared_state_unchanged(config, &before_mount, "baseline mount")?;
                verify_cold_files(
                    &mut controller,
                    "client-post-activation",
                    &[
                        (
                            "compat-dir/offline-baseline.txt",
                            b"accepted-while-postgres-down-sfs-1019\n",
                        ),
                        (post_activate_path, post_activate_bytes),
                    ],
                )?;
                record_op(
                    ops,
                    "successor_healthy_and_shared_state_intact",
                    CellStatus::Passed,
                    "refusals preserved PG state and a fresh successor read both S3 payloads",
                );

                Ok("baseline failed closed / fenced after incompatible activation while preserving local redb and shared PG/S3".to_string())
            },
        )?;

        // Cell 7: Irreversible local redb revision 7 upgrade (`upgrade --local-only`) -> baseline fails closed.
        run_cell(
            config,
            &mut pair.cells,
            "cell_07_local_format_upgrade_irreversible_fail_closed",
            "local_format_fail_closed",
            |ops| {
                controller.run_cli(
                    CLIENT_SUCCESSOR,
                    "successor-local-only-upgrade",
                    vec![
                        "upgrade".into(),
                        config.volume_name.clone(),
                        "--local-only".into(),
                    ],
                )?;
                record_op(
                    ops,
                    "successor_local_redb_upgrade_rev7",
                    CellStatus::Passed,
                    "successor upgraded local redb layout to revision 7 via `upgrade --local-only`",
                );

                // Successor mounts and writes using its upgraded rev-7 local redb.
                controller.mount(CLIENT_SUCCESSOR)?;
                let succ_mount = controller
                    .client_by_name(CLIENT_SUCCESSOR)?
                    .mount_point
                    .clone();
                write_sync(
                    &succ_mount.join("compat-dir/rev7-local.txt"),
                    b"written-with-rev7-local-redb\n",
                )?;
                controller.unmount(CLIENT_SUCCESSOR)?;
                record_op(
                    ops,
                    "successor_mounts_rev7_local_redb",
                    CellStatus::Passed,
                    "successor mounted and wrote cleanly with local redb revision 7",
                );

                // Baseline binary pointed at the rev-7 local redb must fail closed without corrupting it.
                let succ_spec = controller.client_by_name(CLIENT_SUCCESSOR)?.clone();
                let rev7_size_before = fs::metadata(&succ_spec.local_metadata_db)?.len();
                let fail_mount = run_mount_expect_failure(
                    &binaries.baseline_bin,
                    &succ_spec,
                    &context.scenario_dir,
                    EXPECTED_FAIL_TIMEOUT,
                )?;
                assert_refusal(
                    "baseline mount on revision 7 redb",
                    &fail_mount,
                    &[
                        "upgrade required",
                        "unknown formatinfo field `local_redb_revision`",
                    ],
                )?;
                let rev7_size_after = fs::metadata(&succ_spec.local_metadata_db)?.len();
                if rev7_size_after == 0 {
                    return Err(io::Error::other(
                        "baseline mount refusal truncated rev-7 local redb",
                    )
                    .into());
                }
                record_op(
                    ops,
                    "baseline_refuses_rev7_local_redb",
                    CellStatus::Passed,
                    format!(
                        "baseline failed closed on rev-7 local redb (exit {}, size {rev7_size_before} -> {rev7_size_after} bytes)",
                        fail_mount.status
                    ),
                );

                // Re-mount with successor to prove rev-7 local redb is still intact.
                controller.mount(CLIENT_SUCCESSOR)?;
                let got = fs::read(succ_mount.join("compat-dir/rev7-local.txt"))?;
                if got != b"written-with-rev7-local-redb\n" {
                    return Err(io::Error::other(
                        "successor readback failed after baseline rev-7 refusal",
                    )
                    .into());
                }
                controller.unmount(CLIENT_SUCCESSOR)?;
                record_op(
                    ops,
                    "successor_remount_confirms_rev7_intact",
                    CellStatus::Passed,
                    "successor remounted and read rev-7 data after baseline refusal",
                );

                Ok("irreversible local redb revision 7 upgrade and baseline fail-closed refusal passed".to_string())
            },
        )?;

        Ok(())
    })();

    let cleanup = controller.unmount_all().map_err(ScenarioError::from);
    if let Err(error) = &cleanup
        && let Some(cell) = pair.cells.last_mut()
    {
        cell.status = CellStatus::Failed;
        record_op(
            &mut cell.operations,
            "pair_cleanup",
            CellStatus::Failed,
            matrix_error_detail(config, error),
        );
    }
    finish_cleanup("baseline/successor pair", result, cleanup)
}

fn run_format5_adoption_pair(
    base_config: &ClientControllerConfig,
    binaries: &MatrixBinaries,
    baseline: &BinaryIdentityRecord,
    legacy_bin: &Path,
) -> Result<(ReleasePairReport, bool), ScenarioError> {
    let mut pair = ReleasePairReport {
        pair_id: "0.2.2-published-format5-to-0.2.5-baseline".into(),
        baseline: BinaryIdentityRecord {
            label: "v0.2.2-published".into(),
            client_release: "0.2.2".into(),
            binary_sha256: sha256_file(legacy_bin)?,
            successor_build: None,
            identities: Vec::new(),
            declared_changes: Vec::new(),
        },
        successor: baseline.clone(),
        observed_protocol: ObservedProtocolState::default(),
        cells: Vec::new(),
    };
    let mut config = base_config.clone();
    config.volume_name = format!("{}-v022", base_config.volume_name);
    let mut controller = MultiClientController::new(config.clone(), SystemClientProcessExecutor);
    let mut cleanup_failed = false;
    let _ = run_cell(
        &config,
        &mut pair.cells,
        "cell_09_format5_adoption_after_cancellation",
        "fenced_legacy_client",
        |ops| {
            let writer = "client-v022";
            let cold = "client-v025-from-format5";
            let result = (|| {
                controller.create_clients([writer, cold])?;
                controller.set_client_bin_override(writer, legacy_bin.to_path_buf())?;
                controller.init_first_client()?;
                if observe_protocol_state(&config)?.metadata_format_version != Some(5) {
                    return Err(
                        io::Error::other("published v0.2.2 did not initialize format 5").into(),
                    );
                }
                controller.mount(writer)?;
                let mount = &controller.client_by_name(writer)?.mount_point;
                let local_db = controller.client_by_name(writer)?.local_metadata_db.clone();
                let payload = b"written by the published format-5 client\n";
                let before = observe_metadata_version(&config, Duration::from_secs(15))?;
                write_sync(&mount.join("from-format5.txt"), payload)?;
                wait_for_published_write(&controller, writer, &config, before)?;
                controller.unmount(writer)?;

                // The first run commits 5 -> 6 and cancels only 6 -> 7.
                controller.set_client_bin_override(writer, binaries.baseline_bin.clone())?;
                exercise_legacy_adoption_cancellation(&controller, &config, writer, ops)?;
                if RedbMetaEngine::read_format_info_for_upgrade_readonly(&local_db)
                    .map_err(io::Error::other)?
                    .format_version
                    != 5
                {
                    return Err(io::Error::other(
                        "interrupted shared adoption changed the original local format",
                    )
                    .into());
                }
                controller.run_cli(
                    writer,
                    "retry-format5-adoption",
                    vec!["upgrade".into(), config.volume_name.clone()],
                )?;
                let adopted = observe_protocol_state(&config)?;
                if adopted.metadata_format_version != Some(7) || adopted.policy_epoch != Some(1) {
                    return Err(io::Error::other(
                        "retry did not commit format 7 and the initial policy epoch",
                    )
                    .into());
                }
                pair.observed_protocol = adopted;
                controller.mount(writer)?;
                let mount = &controller.client_by_name(writer)?.mount_point;
                let local = bounded_mounted_read(
                    mount.join("from-format5.txt"),
                    "read original format-5 cache after adoption",
                    CONVERGENCE_TIMEOUT,
                )?;
                if !recovered_payload_matches(&local, payload) {
                    return Err(
                        io::Error::other("adoption changed the original format-5 payload").into(),
                    );
                }
                controller.unmount(writer)?;
                verify_cold_files(&mut controller, cold, &[("from-format5.txt", payload)])?;
                record_op(
                    ops,
                    "format5_chain_recovers_original_payload",
                    CellStatus::Passed,
                    "a fresh 0.2.5 mount verified the v0.2.2 payload after 5 -> 6, cancellation, and 6 -> 7 retry",
                );
                Ok("format-5 adoption resumed from its committed intermediate step".to_string())
            })();
            let cleanup = controller.unmount_all().map_err(ScenarioError::from);
            cleanup_failed = cleanup.is_err();
            finish_cleanup("format-5 adoption pair", result, cleanup)
        },
    );
    Ok((pair, cleanup_failed))
}

fn run_fenced_v024_pair(
    base_config: &ClientControllerConfig,
    binaries: &MatrixBinaries,
) -> (ReleasePairReport, bool) {
    let baseline_record = query_version_identities(&binaries.baseline_bin, "0.2.5-baseline")
        .unwrap_or_else(|_| BinaryIdentityRecord {
            label: "0.2.5-baseline".to_string(),
            client_release: "unknown".to_string(),
            binary_sha256: String::new(),
            successor_build: None,
            identities: Vec::new(),
            declared_changes: Vec::new(),
        });

    let Some(v024_bin) = binaries.v024_bin.clone() else {
        let status = if binaries.require_v024 {
            CellStatus::Failed
        } else {
            CellStatus::Skipped
        };
        let detail = format!(
            "published v0.2.4 binary not provided (set {V024_BIN_ENV} or run tools/test_successor/run_compat_matrix.py)"
        );
        return (
            ReleasePairReport {
                pair_id: "0.2.4-published-vs-0.2.5-baseline-fenced".to_string(),
                baseline: BinaryIdentityRecord {
                    label: "v0.2.4-published".to_string(),
                    client_release: "0.2.4".to_string(),
                    binary_sha256: "not-provided".to_string(),
                    successor_build: None,
                    identities: Vec::new(),
                    declared_changes: Vec::new(),
                },
                successor: baseline_record,
                observed_protocol: ObservedProtocolState::default(),
                cells: vec![MatrixCellReport {
                    id: "cell_08_fenced_v024_after_upgrade".to_string(),
                    category: "fenced_legacy_client".to_string(),
                    status: status.clone(),
                    duration_ms: 0,
                    detail: detail.clone(),
                    operations: vec![OperationRecord {
                        name: "v024_binary_resolution".to_string(),
                        status,
                        detail,
                    }],
                }],
            },
            false,
        );
    };

    let v024_sha = sha256_file(&v024_bin).unwrap_or_else(|_| "unknown".to_string());
    let mut pair = ReleasePairReport {
        pair_id: "0.2.4-published-vs-0.2.5-baseline-fenced".to_string(),
        baseline: BinaryIdentityRecord {
            label: "v0.2.4-published".to_string(),
            client_release: "0.2.4".to_string(),
            binary_sha256: v024_sha,
            successor_build: None,
            identities: Vec::new(),
            declared_changes: Vec::new(),
        },
        successor: baseline_record,
        observed_protocol: ObservedProtocolState::default(),
        cells: Vec::new(),
    };

    let mut cleanup_failed = false;
    let _ = run_cell(
        base_config,
        &mut pair.cells,
        "cell_08_fenced_v024_after_upgrade",
        "fenced_legacy_client",
        |ops| {
            // Use a dedicated volume on the Compose PostgreSQL server before the successor changes core schema.
            let mut v024_config = base_config.clone();
            v024_config.volume_name = format!("{}-v024", base_config.volume_name);

            let mut controller =
                MultiClientController::new(v024_config.clone(), SystemClientProcessExecutor);
            controller.create_clients([
                "client-v024-a",
                "client-v024-b",
                "client-v024-fresh",
                "client-v025-cold",
            ])?;
            controller.set_client_bin_override("client-v024-a", v024_bin.clone())?;
            controller.set_client_bin_override("client-v024-b", v024_bin.clone())?;
            controller.set_client_bin_override("client-v024-fresh", v024_bin.clone())?;

            let cell_result = (|| -> Result<String, ScenarioError> {
                // 1. v0.2.4 initializes a format-6 volume and writes data.
                controller.init_first_client()?;
                controller.discover("client-v024-b")?;
                controller.mount("client-v024-a")?;
                let mount_a = controller
                    .client_by_name("client-v024-a")?
                    .mount_point
                    .clone();
                let v024_bytes = b"written-by-published-v0.2.4-before-upgrade\n";
                write_sync(&mount_a.join("from-v024.txt"), v024_bytes)?;
                controller.unmount("client-v024-a")?;

                // Give the client that will be fenced a durable file it created itself.
                let sentinel = Path::new("v024-b-sentinel.txt");
                let sentinel_bytes = b"locally-written-by-v024-b\n";
                controller.mount("client-v024-b")?;
                let mount_b = &controller.client_by_name("client-v024-b")?.mount_point;
                let before_sentinel =
                    observe_metadata_version(&v024_config, Duration::from_secs(15))?;
                write_sync(&mount_b.join(sentinel), sentinel_bytes)?;
                wait_for_published_write(
                    &controller,
                    "client-v024-b",
                    &v024_config,
                    before_sentinel,
                )?;
                controller.unmount("client-v024-b")?;

                let pre_proto = observe_protocol_state(&v024_config)?;
                if pre_proto.metadata_format_version != Some(6) {
                    return Err(io::Error::other(format!(
                        "expected v0.2.4 init to produce format 6, got {:?}",
                        pre_proto.metadata_format_version
                    ))
                    .into());
                }
                record_op(
                    ops,
                    "v024_init_format6_and_write",
                    CellStatus::Passed,
                    "published v0.2.4 initialized format 6 volume and wrote from-v024.txt",
                );

                // Record v0.2.4 client-b's local redb SHA-256 before upgrade.
                let v024_b_db = controller
                    .client_by_name("client-v024-b")?
                    .local_metadata_db
                    .clone();
                let v024_b_before = offline_sentinel(&v024_b_db, sentinel, sentinel_bytes.len())?;
                let v024_b_sha_before = sha256_file(&v024_b_db)?;

                // 2. Switch client-v024-a to 0.2.5 baseline binary and run `studiofs upgrade` (format 6 -> 7).
                controller
                    .set_client_bin_override("client-v024-a", binaries.baseline_bin.clone())?;
                if crash_rollback_enabled() {
                    exercise_legacy_adoption_cancellation(
                        &controller,
                        &v024_config,
                        "client-v024-a",
                        ops,
                    )?;
                }
                controller.run_cli(
                    "client-v024-a",
                    "v025-upgrade-format6-to-7",
                    vec!["upgrade".into(), v024_config.volume_name.clone()],
                )?;
                let post_proto = observe_protocol_state(&v024_config)?;
                if post_proto.metadata_format_version != Some(7) {
                    return Err(io::Error::other(format!(
                        "expected format 7 after 0.2.5 upgrade, got {:?}",
                        post_proto.metadata_format_version
                    ))
                    .into());
                }
                pair.observed_protocol = post_proto;
                record_op(
                    ops,
                    "baseline_025_upgrade_format6_to_7",
                    CellStatus::Passed,
                    "0.2.5 `studiofs upgrade` adopted format 6 -> 7 with volume_protocol and format fences",
                );

                // 3. 0.2.5 mounts the upgraded volume, reads v0.2.4's file, and writes a new file.
                controller.mount("client-v024-a")?;
                let got = fs::read(mount_a.join("from-v024.txt"))?;
                if got != v024_bytes {
                    return Err(io::Error::other(
                        "0.2.5 readback of v0.2.4 file failed after format 6 -> 7 upgrade",
                    )
                    .into());
                }
                let before_write = observe_shared_state(&v024_config)?;
                let v025_bytes = b"written-by-0.2.5-after-format7-adoption\n";
                write_sync(&mount_a.join("from-025.txt"), v025_bytes)?;
                wait_for_published_write(
                    &controller,
                    "client-v024-a",
                    &v024_config,
                    before_write.version,
                )?;
                controller.unmount("client-v024-a")?;
                let before_refusals = observe_shared_state(&v024_config)?;
                record_op(
                    ops,
                    "baseline_025_reads_v024_and_writes_format7",
                    CellStatus::Passed,
                    "0.2.5 read pre-upgrade v0.2.4 data and wrote post-upgrade file",
                );

                // 4. Published v0.2.4 must FAIL CLOSED on fresh discover AND on un-upgraded client-v024-b mount.
                let fresh_discover = controller.run_cli_allow_failure(
                    "client-v024-fresh",
                    "v024-discover-refused-on-format7",
                    vec![
                        "discover".into(),
                        v024_config.volume_name.clone(),
                        "--meta".into(),
                        v024_config.meta_dsn.clone(),
                    ],
                )?;
                assert_refusal(
                    "v0.2.4 discovery on format 7",
                    &fresh_discover,
                    &[FORMAT_7_REFUSAL],
                )?;
                record_op(
                    ops,
                    "v024_discover_refused_on_format7",
                    CellStatus::Passed,
                    format!(
                        "published v0.2.4 `discover` failed closed with exit {}",
                        fresh_discover.status
                    ),
                );

                let fence_detail = if mount_or_verify_refusal(
                    &mut controller,
                    "client-v024-b",
                    "v0.2.4 mount on format 7",
                    &[FORMAT_7_REFUSAL],
                )? {
                    let mount_b = controller
                        .client_by_name("client-v024-b")?
                        .mount_point
                        .clone();
                    let write_result = match write_sync(
                        &mount_b.join("fenced-v024-write.txt"),
                        b"must-not-publish\n",
                    ) {
                        Ok(()) => "local write accepted".to_string(),
                        Err(error) => format!("local write returned {:?}", error.kind()),
                    };
                    wait_for_v024_fence(&controller, "client-v024-b")?;
                    assert_missing_file(&mount_b.join("from-025.txt"))?;
                    controller.unmount("client-v024-b")?;
                    format!("v0.2.4 format-7 fence observed ({write_result})")
                } else {
                    "v0.2.4 mount refused startup on format-7 volume".to_string()
                };
                if offline_sentinel(&v024_b_db, sentinel, sentinel_bytes.len())? != v024_b_before {
                    return Err(io::Error::other(
                        "v0.2.4 sentinel inode or payload map changed after refusal",
                    )
                    .into());
                }
                let v024_b_sha_after = sha256_file(&v024_b_db)?;
                record_op(
                    ops,
                    "v024_mount_fenced_and_local_redb_preserved",
                    CellStatus::Passed,
                    format!(
                        "{fence_detail}; offline read-only redb inspection verified the sentinel inode, size, and payload map (sha_unchanged={})",
                        v024_b_sha_before == v024_b_sha_after
                    ),
                );

                assert_shared_state_unchanged(&v024_config, &before_refusals, "v0.2.4 refusals")?;
                verify_cold_files(
                    &mut controller,
                    "client-v025-cold",
                    &[
                        ("from-v024.txt", v024_bytes),
                        ("from-025.txt", v025_bytes),
                        ("v024-b-sentinel.txt", sentinel_bytes),
                    ],
                )?;
                record_op(
                    ops,
                    "pg_s3_and_025_mount_intact_after_v024_fencing",
                    CellStatus::Passed,
                    "PG state unchanged and fresh 0.2.5 client read all three S3 payloads",
                );

                Ok("published v0.2.4 failed closed after 0.2.5 format 6 -> 7 upgrade without corrupting PG/S3 or local redb".to_string())
            })();

            let cleanup = controller.unmount_all().map_err(ScenarioError::from);
            cleanup_failed = cleanup.is_err();
            if let Err(error) = &cleanup {
                record_op(
                    ops,
                    "pair_cleanup",
                    CellStatus::Failed,
                    matrix_error_detail(base_config, error),
                );
            }
            finish_cleanup("v0.2.4 pair", cell_result, cleanup)
        },
    );

    (pair, cleanup_failed)
}

// Inspect the unmounted database through the existing read-only backend. Opening
// it through the normal mount path could replay PG or repair the evidence.
fn offline_sentinel(
    database: &Path,
    path: &Path,
    expected_size: usize,
) -> Result<(u64, Vec<ResolvedSliceSpan>), ScenarioError> {
    let inspect_error = |error: studiofs_meta::MetaError| {
        io::Error::other(format!("offline sentinel {}: {error}", path.display()))
    };
    let metadata = RedbMetaEngine::open_read_only(database).map_err(inspect_error)?;
    let mut ino = ROOT_INO;
    for name in path.iter() {
        ino = metadata
            .lookup(ino, name.as_encoded_bytes())
            .map_err(inspect_error)?
            .ino;
    }
    let attr = metadata.get_attr(ino).map_err(inspect_error)?;
    if attr.size != expected_size as u64 {
        return Err(
            io::Error::other(format!("offline sentinel {} size mismatch", path.display())).into(),
        );
    }
    let slices = metadata
        .lookup_regular_file_stored_slices(ino, 0, attr.size)
        .map_err(inspect_error)?;
    if !slices.iter().any(|span| span.payload.is_some()) {
        return Err(io::Error::other(format!(
            "offline sentinel {} has no payload",
            path.display()
        ))
        .into());
    }
    Ok((ino, slices))
}

fn write_sync(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn deterministic_bytes(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| seed.wrapping_add((i % 251) as u8).wrapping_mul(31))
        .collect()
}

fn wait_pair_converged(
    context: &ScenarioContext,
    mount_a: &Path,
    mount_b: &Path,
    label: &str,
) -> Result<(), ScenarioError> {
    let mounts = vec![
        ClientMount::new(CLIENT_SUCCESSOR, mount_a.to_path_buf()),
        ClientMount::new(CLIENT_BASELINE, mount_b.to_path_buf()),
    ];
    wait_for_convergence(
        &mounts,
        context.scenario_dir.join(label),
        CONVERGENCE_TIMEOUT,
        POLL_INTERVAL,
    )?;
    Ok(())
}

#[cfg(unix)]
fn exercise_mounted_xattr(
    succ_file: &Path,
    base_file: &Path,
) -> Result<OperationRecord, ScenarioError> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt as _;

    let name = CString::new("user.studiofs.compat").expect("static xattr name");
    let succ_c = CString::new(succ_file.as_os_str().as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let base_c = CString::new(base_file.as_os_str().as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    for (writer, reader, value, direction) in [
        (
            &succ_c,
            &base_c,
            &b"from-successor"[..],
            "successor to baseline",
        ),
        (
            &base_c,
            &succ_c,
            &b"from-baseline"[..],
            "baseline to successor",
        ),
    ] {
        #[cfg(target_os = "macos")]
        let set_rc = unsafe {
            libc::setxattr(
                writer.as_ptr(),
                name.as_ptr(),
                value.as_ptr().cast(),
                value.len(),
                0,
                0,
            )
        };
        #[cfg(not(target_os = "macos"))]
        let set_rc = unsafe {
            libc::setxattr(
                writer.as_ptr(),
                name.as_ptr(),
                value.as_ptr().cast(),
                value.len(),
                0,
            )
        };
        if set_rc != 0 {
            return Err(io::Error::last_os_error().into());
        }

        let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
        let mut buf = [0_u8; 128];
        loop {
            #[cfg(target_os = "macos")]
            let got = unsafe {
                libc::getxattr(
                    reader.as_ptr(),
                    name.as_ptr(),
                    buf.as_mut_ptr().cast(),
                    buf.len(),
                    0,
                    0,
                )
            };
            #[cfg(not(target_os = "macos"))]
            let got = unsafe {
                libc::getxattr(
                    reader.as_ptr(),
                    name.as_ptr(),
                    buf.as_mut_ptr().cast(),
                    buf.len(),
                )
            };
            if got >= 0 && &buf[..got as usize] == value {
                break;
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "timed out waiting for user.studiofs.compat xattr convergence ({direction})"
                    ),
                )
                .into());
            }
            std::thread::sleep(POLL_INTERVAL);
        }
    }
    Ok(OperationRecord {
        name: "mounted_xattr_two_way".to_string(),
        status: CellStatus::Passed,
        detail: "user.studiofs.compat changes converged in both directions".to_string(),
    })
}

#[cfg(not(unix))]
fn exercise_mounted_xattr(
    _succ_file: &Path,
    _base_file: &Path,
) -> Result<OperationRecord, ScenarioError> {
    Ok(OperationRecord {
        name: "mounted_xattr_two_way".to_string(),
        status: CellStatus::Skipped,
        detail: "platform-gated: WinFSP does not expose POSIX setxattr/getxattr syscalls on Windows; xattr protocol capability is covered in cell_05/cell_06"
            .to_string(),
    })
}

fn verify_cold_files(
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
    name: &str,
    files: &[(&str, &[u8])],
) -> Result<(), ScenarioError> {
    controller.discover(name)?;
    controller.mount(name)?;
    let readback = (|| {
        let mount = &controller.client_by_name(name)?.mount_point;
        for (path, expected) in files {
            if bounded_mounted_read(
                mount.join(path),
                format!("cold read {name}/{path}"),
                CONVERGENCE_TIMEOUT,
            )? != *expected
            {
                return Err(
                    io::Error::other(format!("cold client readback mismatch for {path}")).into(),
                );
            }
        }
        Ok(())
    })();
    finish_cleanup(
        "cold client readback",
        readback,
        controller.unmount(name).map_err(Into::into),
    )
}

fn assert_missing_file(path: &Path) -> Result<(), ScenarioError> {
    if path.try_exists()? {
        return Err(io::Error::other(format!("fenced client replayed {}", path.display())).into());
    }
    Ok(())
}

fn wait_for_matrix_state(
    description: &str,
    timeout: Duration,
    mut observe: impl FnMut(Instant) -> Result<bool, ScenarioError>,
) -> Result<(), ScenarioError> {
    let deadline = Instant::now() + timeout;
    let mut last_error = None;
    while Instant::now() < deadline {
        match observe(deadline) {
            Ok(true) if Instant::now() < deadline => return Ok(()),
            Ok(_) => (),
            Err(error) if is_transient_observation_error(&error) => last_error = Some(error),
            Err(error) => return Err(error),
        }
        std::thread::sleep(POLL_INTERVAL.min(remaining_observation_time(deadline)));
    }
    let detail = last_error.map_or_else(String::new, |error| {
        format!("; last readiness error: {error}")
    });
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        format!("timed out waiting for {description}{detail}"),
    )
    .into())
}

fn remaining_observation_time(deadline: Instant) -> Duration {
    deadline
        .saturating_duration_since(Instant::now())
        .min(Duration::from_secs(15))
}

fn is_transient_observation_error(error: &ScenarioError) -> bool {
    match error {
        ScenarioError::Io(error) | ScenarioError::Client(ClientControllerError::Io(error)) => {
            matches!(
                error.kind(),
                io::ErrorKind::TimedOut
                    | io::ErrorKind::ConnectionRefused
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::BrokenPipe
            )
        }
        ScenarioError::Client(ClientControllerError::CommandFailed {
            status: 1, stderr, ..
        }) => stderr
            .lines()
            .any(|line| line.starts_with("Error: no live diagnostics session found for ")),
        _ => false,
    }
}

fn wait_for_runtime_fence(
    controller: &MultiClientController<SystemClientProcessExecutor>,
    name: &str,
) -> Result<(), ScenarioError> {
    wait_for_matrix_state(
        "baseline degraded with an upgrade-required sync error",
        Duration::from_secs(20),
        |deadline| {
            let snapshot = controller
                .runtime_snapshot_with_timeout(name, remaining_observation_time(deadline))?;
            Ok({
                let sync = &snapshot["synchronization"]["state"]["pg_sync"];
                sync["lifecycle"] == "degraded"
                    && sync["write_authority"] == false
                    && sync["last_sync_error"] == "upgrade_required"
            })
        },
    )
}

fn wait_for_v024_fence(
    controller: &MultiClientController<SystemClientProcessExecutor>,
    name: &str,
) -> Result<(), ScenarioError> {
    let client = controller.client_by_name(name)?;
    wait_for_matrix_state(
        "v0.2.4 format-7 refusal in mount logs",
        CONVERGENCE_TIMEOUT,
        |_| {
            for path in client_daemon_log_paths(client)? {
                match fs::read_to_string(path) {
                    Ok(log) if log.to_ascii_lowercase().contains(FORMAT_7_REFUSAL) => {
                        return Ok(true);
                    }
                    Ok(_) => (),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => (),
                    Err(error) => return Err(error.into()),
                }
            }
            Ok(false)
        },
    )
}

#[derive(Debug, PartialEq, Eq)]
struct SharedState {
    version: i64,
    event_count: i64,
    format: i16,
    policy_epoch: Option<i64>,
    publication_epoch: i64,
}

fn observe_shared_state(config: &ClientControllerConfig) -> Result<SharedState, ScenarioError> {
    let volume_name = config.volume_name.clone();
    with_pg_timeout(
        config,
        "observe shared state",
        Some(Duration::from_secs(15)),
        |client| async move {
            let row = client
                .query_one(
                    "SELECT m.version, (SELECT count(*) FROM event_log WHERE vol = m.vol),
                    m.metadata_format_version, p.policy_epoch, m.publication_epoch
             FROM metadata_state m JOIN volumes v ON v.vol = m.vol
             LEFT JOIN volume_protocol p ON p.vol = m.vol WHERE v.name = $1",
                    &[&volume_name],
                )
                .await
                .map_err(pg_err("read shared state"))?;
            let decode = pg_err("decode shared state");
            Ok(SharedState {
                version: row.try_get(0).map_err(&decode)?,
                event_count: row.try_get(1).map_err(&decode)?,
                format: row.try_get(2).map_err(&decode)?,
                policy_epoch: row.try_get(3).map_err(&decode)?,
                publication_epoch: row.try_get(4).map_err(&decode)?,
            })
        },
    )
}

fn assert_shared_state_unchanged(
    config: &ClientControllerConfig,
    before: &SharedState,
    action: &str,
) -> Result<(), ScenarioError> {
    let after = observe_shared_state(config)?;
    if after != *before {
        return Err(io::Error::other(format!(
            "{action} changed shared PG state: {before:?} -> {after:?}"
        ))
        .into());
    }
    Ok(())
}

fn observe_metadata_version(
    config: &ClientControllerConfig,
    timeout: Duration,
) -> Result<i64, ScenarioError> {
    let volume_name = config.volume_name.clone();
    // This query also runs against format 6, before volume_protocol exists.
    with_pg_timeout(
        config,
        "observe metadata version",
        Some(timeout),
        |client| async move {
            let row = client.query_one(
            "SELECT m.version FROM metadata_state m JOIN volumes v ON v.vol = m.vol WHERE v.name = $1",
            &[&volume_name],
        ).await.map_err(pg_err("read metadata version"))?;
            row.try_get(0).map_err(pg_err("decode metadata version"))
        },
    )
}

fn wait_for_published_write(
    controller: &MultiClientController<SystemClientProcessExecutor>,
    writer: &str,
    config: &ClientControllerConfig,
    version: i64,
) -> Result<(), ScenarioError> {
    wait_for_matrix_state(
        "compatible client publication and an advanced metadata_state.version",
        CONVERGENCE_TIMEOUT,
        |deadline| {
            let runtime = controller
                .runtime_snapshot_with_timeout(writer, remaining_observation_time(deadline))?;
            Ok(runtime["sync_safety"]["state"] == "safe"
                && runtime["upload"]["pending_uploads"] == 0
                && runtime["upload"]["active_uploads"] == 0
                && observe_metadata_version(config, remaining_observation_time(deadline))?
                    > version)
        },
    )
}

fn optional_pg_observation<T>(
    result: Result<T, tokio_postgres::Error>,
    operation: &str,
) -> Result<Option<T>, ScenarioError> {
    use tokio_postgres::error::SqlState;
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error)
            if matches!(
                error.code(),
                Some(&SqlState::UNDEFINED_TABLE | &SqlState::UNDEFINED_COLUMN)
            ) =>
        {
            Ok(None)
        }
        Err(error) => Err(pg_err(operation)(error)),
    }
}

fn pg_err(op: &str) -> impl Fn(tokio_postgres::Error) -> ScenarioError + '_ {
    move |error| io::Error::new(pg_observation_error_kind(&error), format!("{op}: {error}")).into()
}

fn observe_protocol_state(
    config: &ClientControllerConfig,
) -> Result<ObservedProtocolState, ScenarioError> {
    let volume_name = config.volume_name.clone();
    with_pg(config, "observe protocol state", |client| async move {
        let components = client
            .query(
                "SELECT component, version FROM studiofs_schema_components ORDER BY component",
                &[],
            )
            .await
            .map_err(pg_err("select studiofs_schema_components"))?
            .into_iter()
            .map(|row| SchemaComponentVersion {
                component: row.get(0),
                version: row.get(1),
            })
            .collect::<Vec<_>>();

        let state = client
            .query_opt(
                "SELECT m.vol, m.metadata_format_version, m.checkpoint_version, m.event_log_prune_floor
                 FROM metadata_state m JOIN volumes v ON v.vol = m.vol WHERE v.name = $1",
                &[&volume_name],
            )
            .await
            .map_err(pg_err("select metadata_state"))?;

        let Some(state) = state else {
            return Ok(ObservedProtocolState {
                schema_components: components,
                ..Default::default()
            });
        };
        let vol: i64 = state.get(0);
        let metadata_format_version: i16 = state.get(1);
        let checkpoint_version: i64 = state.get(2);
        let event_log_prune_floor: i64 = state.get(3);

        // These protocol fields and encodings can be absent on the pre-upgrade v0.2.4 schema.
        let write_requirement = optional_pg_observation(
            client
                .query_opt(
                    "SELECT write_requirement FROM metadata_state WHERE vol = $1",
                    &[&vol],
                )
                .await,
            "select write_requirement",
        )?
        .flatten()
        .map(|row| row.try_get::<_, i16>(0))
        .transpose()
        .map_err(pg_err("decode write_requirement"))?;

        let policy_epoch = optional_pg_observation(
            client
                .query_opt(
                    "SELECT policy_epoch FROM volume_protocol WHERE vol = $1",
                    &[&vol],
                )
                .await,
            "select policy_epoch",
        )?
        .flatten()
        .map(|row| row.try_get::<_, i64>(0))
        .transpose()
        .map_err(pg_err("decode policy_epoch"))?;

        let mut ckpt_formats = BTreeSet::new();
        if checkpoint_version > 0 {
            ckpt_formats.insert(4_i16);
        }
        if let Some(rows) = optional_pg_observation(client
            .query(
                "SELECT DISTINCT checkpoint_format FROM serving_checkpoint_encodings WHERE vol = $1 ORDER BY checkpoint_format",
                &[&vol],
            )
            .await, "select serving_checkpoint_encodings")?
        {
            for row in rows {
                ckpt_formats.insert(row.try_get::<_, i16>(0).map_err(pg_err("decode checkpoint_format"))?);
            }
        }

        let event_types = client
            .query(
                "SELECT DISTINCT event_type FROM event_log WHERE vol = $1 ORDER BY event_type",
                &[&vol],
            )
            .await
            .map_err(pg_err("select event_log"))?
            .into_iter()
            .map(|row| row.try_get::<_, String>(0))
            .collect::<Result<Vec<_>, _>>()
            .map_err(pg_err("decode event_type"))?;

        Ok(ObservedProtocolState {
            metadata_format_version: Some(metadata_format_version),
            write_requirement,
            policy_epoch,
            checkpoint_version: Some(checkpoint_version),
            event_log_prune_floor: Some(event_log_prune_floor),
            schema_components: components,
            checkpoint_formats_observed: ckpt_formats.into_iter().collect(),
            event_types_observed: event_types,
        })
    })
}

fn wait_for_dual_checkpoint_and_prune(
    config: &ClientControllerConfig,
    timeout: Duration,
    before: Option<&ObservedProtocolState>,
) -> Result<ObservedProtocolState, ScenarioError> {
    let previous_checkpoint = before
        .and_then(|state| state.checkpoint_version)
        .unwrap_or(0);
    let previous_floor = before
        .and_then(|state| state.event_log_prune_floor)
        .unwrap_or(0);
    let deadline = Instant::now() + timeout;
    loop {
        let proto = observe_protocol_state(config)?;
        let has_f4 = proto.checkpoint_formats_observed.contains(&4);
        let has_f5 = proto.checkpoint_formats_observed.contains(&5);
        let pruned = proto.event_log_prune_floor.unwrap_or(0) > previous_floor;
        if proto.checkpoint_version.unwrap_or(0) > previous_checkpoint && has_f4 && has_f5 && pruned
        {
            return Ok(proto);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "timed out waiting for dual checkpoint (format 4 + 5) and event pruning; observed checkpoint_version={:?}, formats={:?}, prune_floor={:?}",
                    proto.checkpoint_version,
                    proto.checkpoint_formats_observed,
                    proto.event_log_prune_floor
                ),
            )
            .into());
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_snapshot_ready(
    config: &ClientControllerConfig,
    snapshot_id: &str,
    timeout: Duration,
) -> Result<(), ScenarioError> {
    let deadline = Instant::now() + timeout;
    loop {
        let volume_name = config.volume_name.clone();
        let snap = snapshot_id.to_string();
        let lifecycle = with_pg(config, "poll snapshot ready", |client| async move {
            let row = client
                .query_opt(
                    "SELECT c.lifecycle FROM snapshot_catalog c JOIN volumes v ON v.vol = c.vol WHERE v.name = $1 AND c.id = $2",
                    &[&volume_name, &snap],
                )
                .await
                .map_err(pg_err("poll snapshot_catalog"))?;
            Ok(row.map(|r| r.get::<_, String>(0)))
        })?;
        if lifecycle.as_deref() == Some("ready") {
            return Ok(());
        }
        if lifecycle.as_deref() == Some("failed") {
            return Err(
                io::Error::other(format!("snapshot {snapshot_id} entered failed state")).into(),
            );
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for snapshot {snapshot_id} ready (last={lifecycle:?})"),
            )
            .into());
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_active_sessions(
    config: &ClientControllerConfig,
    min_active: i64,
    timeout: Duration,
) -> Result<(), ScenarioError> {
    let deadline = Instant::now() + timeout;
    loop {
        let volume_name = config.volume_name.clone();
        let count = with_pg(config, "poll active sessions", |client| async move {
            let row = client
                .query_one(
                    "SELECT count(*) FROM client_sessions s JOIN volumes v ON v.vol = s.vol WHERE v.name = $1 AND s.lease_expires_at > now()",
                    &[&volume_name],
                )
                .await
                .map_err(pg_err("poll client_sessions"))?;
            Ok(row.get::<_, i64>(0))
        })?;
        if count >= min_active {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for {min_active} active sessions (observed {count})"),
            )
            .into());
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

fn active_baseline_sessions(config: &ClientControllerConfig) -> Result<Vec<String>, ScenarioError> {
    let volume_name = config.volume_name.clone();
    with_pg_timeout(
        config,
        "identify baseline lease",
        Some(Duration::from_secs(15)),
        |client| async move {
            let rows = client
                .query(
                    "SELECT s.session_id FROM client_sessions s JOIN volumes v ON v.vol = s.vol
             WHERE v.name = $1 AND s.lease_expires_at > now()
               AND NOT (s.capabilities::jsonb -> 'read' ? $2)",
                    &[&volume_name, &SUCCESSOR_XATTR_CAPABILITY],
                )
                .await
                .map_err(pg_err("read active baseline sessions"))?;
            rows.iter()
                .map(|row| row.try_get(0).map_err(pg_err("decode baseline session ID")))
                .collect()
        },
    )
}

fn expire_all_client_sessions(config: &ClientControllerConfig) -> Result<(), ScenarioError> {
    let volume_name = config.volume_name.clone();
    with_pg(config, "expire client sessions", |client| async move {
        client
            .execute(
                "UPDATE client_sessions SET lease_expires_at = now() - interval '5 minutes'
                 WHERE vol = (SELECT vol FROM volumes WHERE name = $1)",
                &[&volume_name],
            )
            .await
            .map_err(pg_err("expire client_sessions"))?;
        Ok(())
    })
}

struct SnapshotProc {
    child: Child,
    job: ChildJob,
}

fn spawn_snapshot_mount(
    bin: &Path,
    client: &ClientSpec,
    volume: &str,
    snapshot_id: &str,
    mount: &Path,
    scenario_dir: &Path,
) -> Result<SnapshotProc, ScenarioError> {
    let args = snapshot_mount_args(volume, snapshot_id, mount);
    let mut cmd = Command::new(bin);
    cmd.args(args)
        .arg("--cache-dir")
        .arg(&client.cache_dir)
        .env("STUDIOFS_CLIENT_ROOT", &client.client_root)
        .stdout(fs::File::create(
            scenario_dir.join("snapshot-mount.stdout.log"),
        )?)
        .stderr(fs::File::create(
            scenario_dir.join("snapshot-mount.stderr.log"),
        )?);
    spawn_tracked_mount(&mut cmd)
}

fn spawn_tracked_mount(cmd: &mut Command) -> Result<SnapshotProc, ScenarioError> {
    ChildJob::prepare_command(cmd)?;
    let mut child = cmd.spawn()?;
    let job = match ChildJob::assign_to_child(&child) {
        Ok(job) => job,
        Err(error) => {
            let _ = child.kill();
            let cleanup = wait_for_child_exit(&mut child, Duration::from_secs(5))
                .map_err(ScenarioError::from)
                .and_then(require_child_exit);
            return finish_cleanup("mount job assignment", Err(error.into()), cleanup);
        }
    };
    Ok(SnapshotProc { child, job })
}

fn wait_for_mount_active(mount: &Path, timeout: Duration) -> Result<(), ScenarioError> {
    let deadline = Instant::now() + timeout;
    loop {
        if mount_point_is_active(mount)? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for mount point {}", mount.display()),
            )
            .into());
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

fn unmount_snapshot_mount(
    bin: &Path,
    client: &ClientSpec,
    mount: &Path,
    proc: &mut SnapshotProc,
) -> Result<(), ScenarioError> {
    let command = SystemCommandExecutor.run(&CommandSpec {
        program: bin.display().to_string(),
        args: vec!["umount".into(), mount.display().to_string()],
        env: vec![(
            "STUDIOFS_CLIENT_ROOT".into(),
            client.client_root.display().to_string(),
        )],
        cwd: None,
        timeout: Some(Duration::from_secs(10)),
    });
    if wait_for_child_exit(&mut proc.child, Duration::from_secs(10))? {
        require_mount_detached(mount)
    } else {
        let stop = stop_mount_probe(proc).and_then(|()| require_mount_detached(mount));
        let graceful = match command {
            Ok(output) => Err(io::Error::other(format!(
                "mount stayed alive after umount (exit {})",
                output.status
            ))
            .into()),
            Err(error) => Err(error.into()),
        };
        finish_cleanup("graceful probe unmount", graceful, stop)
    }
}

fn require_mount_detached(mount: &Path) -> Result<(), ScenarioError> {
    if mount_point_is_active(mount)? {
        return Err(
            io::Error::other(format!("probe mount {} is still active", mount.display())).into(),
        );
    }
    Ok(())
}

fn require_child_exit(exited: bool) -> Result<(), ScenarioError> {
    if !exited {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "mount probe still alive after termination",
        )
        .into());
    }
    Ok(())
}

fn stop_mount_probe(proc: &mut SnapshotProc) -> Result<(), ScenarioError> {
    let _ = proc.job.terminate();
    let _ = proc.job.force_kill();
    let _ = proc.child.kill();
    require_child_exit(wait_for_child_exit(
        &mut proc.child,
        Duration::from_secs(5),
    )?)
}

fn run_mount_expect_failure(
    bin: &Path,
    client: &ClientSpec,
    scenario_dir: &Path,
    timeout: Duration,
) -> Result<ClientCommandOutput, ScenarioError> {
    let stdout = scenario_dir.join("rev7-refusal.stdout.log");
    let stderr = scenario_dir.join("rev7-refusal.stderr.log");
    let mut cmd = Command::new(bin);
    cmd.args([
        "mount",
        &client.volume_name,
        &client.mount_point.display().to_string(),
        "--cache-dir",
        &client.cache_dir.display().to_string(),
        "--foreground",
    ])
    .env("STUDIOFS_CLIENT_ROOT", &client.client_root)
    .stdout(fs::File::create(&stdout)?)
    .stderr(fs::File::create(&stderr)?);
    let mut proc = spawn_tracked_mount(&mut cmd)?;
    let deadline = Instant::now() + timeout;
    let result = (|| {
        loop {
            if let Some(status) = proc.child.try_wait()? {
                return Ok(ClientCommandOutput {
                    status: status.code().unwrap_or(-1),
                    stdout: String::from_utf8_lossy(&fs::read(stdout)?).into_owned(),
                    stderr: String::from_utf8_lossy(&fs::read(stderr)?).into_owned(),
                });
            }
            if mount_point_is_active(&client.mount_point)? {
                return Err(io::Error::other(format!(
                    "expected mount for {} to fail closed, but mount point {} became active",
                    client.name,
                    client.mount_point.display()
                ))
                .into());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "expected mount for {} to fail closed within {}s, but it stayed alive",
                        client.name,
                        timeout.as_secs()
                    ),
                )
                .into());
            }
            std::thread::sleep(
                POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())),
            );
        }
    })();
    if result.is_err() {
        let cleanup = match mount_point_is_active(&client.mount_point) {
            Ok(true) => unmount_snapshot_mount(bin, client, &client.mount_point, &mut proc),
            Ok(false) => stop_mount_probe(&mut proc),
            Err(error) => finish_cleanup(
                "probe mount state",
                Err(error.into()),
                stop_mount_probe(&mut proc),
            ),
        };
        return finish_cleanup("refusal probe", result, cleanup);
    }
    result
}

pub(crate) fn validate_matrix_report(report: &ForwardCompatMatrixReport) -> Vec<String> {
    let mut issues = Vec::new();
    if report.schema_version != 1 {
        issues.push(format!(
            "unexpected schema_version {}",
            report.schema_version
        ));
    }
    if !report.passed {
        issues.push("matrix report marked passed=false".to_string());
    }
    if report.summary.failed > 0 {
        issues.push(format!("{} failed cell(s)", report.summary.failed));
    }
    if report.pairs.is_empty() {
        issues.push("matrix report contains no release pairs".to_string());
    }
    for pair in &report.pairs {
        if pair.baseline.binary_sha256.is_empty() {
            issues.push(format!(
                "pair {} missing baseline binary_sha256",
                pair.pair_id
            ));
        }
        if pair.successor.binary_sha256.is_empty() {
            issues.push(format!(
                "pair {} missing successor binary_sha256",
                pair.pair_id
            ));
        }
        if pair.cells.is_empty() {
            issues.push(format!("pair {} contains no cells", pair.pair_id));
        }
    }
    if contains_unredacted_diagnostics(report) {
        issues.push("matrix report contains a forbidden token".to_string());
    }
    issues
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refusal_requires_a_failing_exit_and_the_expected_diagnostic() {
        let mut output = ClientCommandOutput {
            status: 1,
            stdout: String::new(),
            stderr: "unreachable PostgreSQL".to_string(),
        };
        assert!(assert_refusal("probe", &output, &[FORMAT_7_REFUSAL]).is_err());
        output.stderr = FORMAT_7_REFUSAL.to_string();
        output.status = 0;
        assert!(assert_refusal("probe", &output, &[FORMAT_7_REFUSAL]).is_err());
        output.status = 1;
        assert!(assert_refusal("probe", &output, &[FORMAT_7_REFUSAL]).is_ok());
    }

    fn sample_identity(label: &str, sha: &str, successor: bool) -> BinaryIdentityRecord {
        BinaryIdentityRecord {
            label: label.to_string(),
            client_release: "0.2.5".to_string(),
            binary_sha256: sha.to_string(),
            successor_build: successor.then(|| EXPECTED_SUCCESSOR_MARKER.to_string()),
            identities: vec![serde_json::json!({
                "name": "shared_volume_format",
                "current": 7,
                "min_supported": 7
            })],
            declared_changes: if successor {
                vec![
                    serde_json::json!({"id": "c1", "kind": "postgres_migration", "rollback": "irreversible"}),
                    serde_json::json!({"id": "c2", "kind": "event_kind", "rollback": "irreversible"}),
                    serde_json::json!({"id": "c3", "kind": "checkpoint_format", "rollback": "reversible"}),
                    serde_json::json!({"id": "c4", "kind": "local_format", "rollback": "irreversible"}),
                ]
            } else {
                Vec::new()
            },
        }
    }

    #[test]
    fn verify_successor_contract_accepts_complete_pair_and_rejects_gaps() {
        let base = sample_identity("0.2.5-baseline", "aaa", false);
        let succ = sample_identity("compiled-test-successor", "bbb", true);
        verify_successor_contract(&base, &succ).expect("complete pair should pass");

        let mut missing_kind = succ.clone();
        missing_kind.declared_changes.pop();
        assert!(verify_successor_contract(&base, &missing_kind).is_err());

        let same_sha = sample_identity("compiled-test-successor", "aaa", true);
        assert!(verify_successor_contract(&base, &same_sha).is_err());
    }

    #[test]
    fn redacts_quoted_and_encoded_credentials_from_command_diagnostics() {
        let mut config = super::super::tests::_test_config(Path::new("."));
        config.s3.access_key = "mount".into();
        config.s3.secret_key = "a\"b+c".into();
        let error = ScenarioError::from(ClientControllerError::CommandFailed {
            client: "client-a".into(),
            action: "mount".into(),
            status: 1,
            stdout: r#"secret a%22b%2Bc and {"secret":"a\"b+c"}"#.into(),
            stderr: "access=mount secret=\"a\"b+c\"".into(),
        });

        let detail = matrix_error_detail(&config, &error);
        let serialized = serde_json::to_string(&detail).unwrap();
        assert!(detail.starts_with("client command mount for client-a"));
        assert!(detail.contains("access=<redacted-access-key>"));
        assert_eq!(detail.matches("<redacted-secret-key>").count(), 2);
        assert!(!serialized.contains("a%22b%2Bc"));
        assert!(!detail.contains(r#"a\"b+c"#));
        let quoted = serde_json::to_string(&config.s3.secret_key).unwrap();
        assert!(!serialized.contains(&quoted[1..quoted.len() - 1]));
    }

    #[test]
    fn validate_matrix_report_checks_structure_and_redaction() {
        let base = sample_identity("0.2.5-baseline", "aaa", false);
        let succ = sample_identity("compiled-test-successor", "bbb", true);
        let mut report = ForwardCompatMatrixReport {
            schema_version: 1,
            scenario: MOUNTED_FORWARD_COMPAT_MATRIX.to_string(),
            platform: "windows-winfsp".to_string(),
            generated_at: "2026-11-27T00:00:00Z".to_string(),
            source_commit: Some("deadbeef".to_string()),
            source_dirty: Some(false),
            pairs: vec![ReleasePairReport {
                pair_id: "0.2.5-baseline-vs-compiled-test-successor".to_string(),
                baseline: base,
                successor: succ,
                observed_protocol: ObservedProtocolState::default(),
                cells: vec![MatrixCellReport {
                    id: "cell_01".to_string(),
                    category: "supported_concurrent_rw".to_string(),
                    status: CellStatus::Passed,
                    duration_ms: 10,
                    detail: "ok".to_string(),
                    operations: vec![OperationRecord {
                        name: "two_way_rw".to_string(),
                        status: CellStatus::Passed,
                        detail: "ok".to_string(),
                    }],
                }],
            }],
            future_pairs_slot: "Reserved for 0.2.5/0.2.7 (SFS-1053)".to_string(),
            summary: MatrixSummary {
                total_cells: 1,
                passed: 1,
                failed: 0,
                skipped: 0,
            },
            passed: true,
        };
        assert!(validate_matrix_report(&report).is_empty());

        report.pairs[0].cells[0].detail = "postgres://studiofs:studiofs@127.0.0.1:5432".to_string();
        let issues = validate_matrix_report(&report);
        assert!(issues.iter().any(|issue| issue.contains("forbidden token")));
    }
}

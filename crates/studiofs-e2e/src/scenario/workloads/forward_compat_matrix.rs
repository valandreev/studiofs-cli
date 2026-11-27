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
use studiofs_meta::schema::{DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE};

use super::recoverable_trash_janitor::with_pg;
use super::snapshot_v2::{
    parse_snapshot_id, redact_diagnostics, snapshot_create_args, snapshot_mount_args,
};
use super::*;
use crate::client::{
    ClientCommandOutput, ClientControllerConfig, ClientControllerError,
    available_snapshot_mount_point, mount_point_is_active,
};
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
const REQUIRE_V024_ENV: &str = "STUDIOFS_E2E_REQUIRE_V024";
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
        let v024_pair = run_fenced_v024_pair(&config, &binaries);

        let mut primary_pair = ReleasePairReport {
            pair_id: "0.2.5-baseline-vs-compiled-test-successor".to_string(),
            baseline: baseline_id,
            successor: successor_id,
            observed_protocol: ObservedProtocolState::default(),
            cells: Vec::new(),
        };

        let primary_result =
            run_baseline_successor_pair(context, &config, &compose, &binaries, &mut primary_pair);

        let pairs = vec![primary_pair, v024_pair];
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
        write_matrix_report(
            &context.scenario_dir.join(MATRIX_ARTIFACT),
            &report,
            &config,
        )?;

        primary_result
            .map_err(|error| io::Error::other(redact_diagnostics(&config, &error.to_string())))?;
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
    config: &ClientControllerConfig,
) -> Result<(), ScenarioError> {
    let json = serde_json::to_string_pretty(report)?;
    let has_s3_keys = [&config.s3.access_key, &config.s3.secret_key]
        .into_iter()
        .filter(|key| !key.is_empty())
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .any(|key| json.contains(&key[1..key.len() - 1]));
    if has_s3_keys
        || contains_unredacted_diagnostics(&json)
        || redact_diagnostics(config, &json) != json
    {
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
    for operation in &mut ops {
        operation.detail = redact_diagnostics(config, &operation.detail);
    }
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
                detail: redact_diagnostics(config, &detail),
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
                detail: redact_diagnostics(config, &error.to_string()),
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
            "{action}: expected CLI refusal with exit 1 and {expected:?}, got exit {}: {text}",
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

fn contains_unredacted_diagnostics(serialized: &str) -> bool {
    ["postgres://", "postgresql://", "rustfsadmin"]
        .iter()
        .any(|token| serialized.contains(token))
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
                // Successor initializes the shared volume (applies core=2 + studiofs_successor_probe).
                controller.init_first_client()?;
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

                // Generate a few batches so checkpoint coordinator publishes format 4 + format 5 and prunes.
                for idx in 0..4 {
                    let path = succ_mount.join(format!("compat-dir/ckpt-batch-{idx}.txt"));
                    write_sync(&path, format!("checkpoint-batch-{idx}\n").as_bytes())?;
                }
                wait_pair_converged(context, &succ_mount, &base_mount, "cell02-pre-checkpoint")?;

                let proto = wait_for_dual_checkpoint_and_prune(config, CONVERGENCE_TIMEOUT)?;
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
                wait_for_convergence(
                    &mounts,
                    context.scenario_dir.join("cell02-cold-bootstrap"),
                    CONVERGENCE_TIMEOUT,
                    POLL_INTERVAL,
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
                snap_verify?;
                snap_unmount?;
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
                write_sync(
                    &base_mount.join("compat-dir/offline-baseline.txt"),
                    offline_bytes,
                )?;
                let local_read = fs::read(base_mount.join("compat-dir/offline-baseline.txt"))?;
                if local_read != offline_bytes {
                    let _ = compose.start_service(POSTGRES_SERVICE_NAME, &executor);
                    return Err(
                        io::Error::other("offline local readback mismatch on baseline").into(),
                    );
                }
                record_op(
                    ops,
                    "offline_local_write_accepted",
                    CellStatus::Passed,
                    "baseline accepted and read back local write while PostgreSQL was stopped",
                );

                compose.start_service(POSTGRES_SERVICE_NAME, &executor)?;
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
                controller.remount(CLIENT_BASELINE)?;
                let base_mount = controller
                    .client_by_name(CLIENT_BASELINE)?
                    .mount_point
                    .clone();
                write_sync(
                    &base_mount.join("compat-dir/post-deactivate.txt"),
                    b"reversible-rollback-ok\n",
                )?;
                controller.unmount(CLIENT_BASELINE)?;
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
                controller.run_cli(
                    CLIENT_SUCCESSOR,
                    "cutover-activate-accept-offline",
                    vec![
                        "upgrade".into(),
                        config.volume_name.clone(),
                        "--feature".into(),
                        SUCCESSOR_XATTR_CAPABILITY.into(),
                        "--phase".into(),
                        "activate".into(),
                        "--role".into(),
                        "read".into(),
                        "--accept-offline-clients".into(),
                    ],
                )?;
                let post_activate = observe_protocol_state(config)?;
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
                let baseline_db_size_before = fs::metadata(&baseline_db)?.len();
                let before_discover = observe_shared_state(config)?;

                // 1. `studiofs compatibility` on baseline must report upgrade_required.
                let compat_out = controller.run_cli_allow_failure(
                    CLIENT_BASELINE,
                    "baseline-compatibility-after-activate",
                    vec!["compatibility".into(), config.volume_name.clone()],
                )?;
                let combined_compat =
                    format!("{}\n{}", compat_out.stdout, compat_out.stderr).to_lowercase();
                if !combined_compat.contains("upgrade_required") {
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

                // 5. Baseline local redb must remain intact (not deleted or truncated to 0).
                let baseline_db_size_after = fs::metadata(&baseline_db)?.len();
                if baseline_db_size_after == 0
                    || baseline_db_size_after < baseline_db_size_before / 2
                {
                    return Err(io::Error::other(format!(
                        "baseline local redb unexpectedly shrank after refusal ({baseline_db_size_before} -> {baseline_db_size_after})"
                    ))
                    .into());
                }
                record_op(
                    ops,
                    "baseline_local_redb_preserved",
                    CellStatus::Passed,
                    format!("local redb preserved ({baseline_db_size_after} bytes)"),
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

    let _ = controller.unmount_all();
    result
}

fn run_fenced_v024_pair(
    base_config: &ClientControllerConfig,
    binaries: &MatrixBinaries,
) -> ReleasePairReport {
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
        return ReleasePairReport {
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
        };
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
                let v024_b_sha_before = sha256_file(&v024_b_db)?;

                // 2. Switch client-v024-a to 0.2.5 baseline binary and run `studiofs upgrade` (format 6 -> 7).
                controller
                    .set_client_bin_override("client-v024-a", binaries.baseline_bin.clone())?;
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

                match mount_or_verify_refusal(
                    &mut controller,
                    "client-v024-b",
                    "v0.2.4 mount on format 7",
                    &[FORMAT_7_REFUSAL],
                )? {
                    false => {
                        let v024_b_sha_after = sha256_file(&v024_b_db)?;
                        if !v024_b_db.is_file() || fs::metadata(&v024_b_db)?.len() == 0 {
                            return Err(io::Error::other(
                                "v0.2.4 local redb was lost or emptied after mount refusal",
                            )
                            .into());
                        }
                        record_op(
                            ops,
                            "v024_mount_fenced_and_local_redb_preserved",
                            CellStatus::Passed,
                            format!(
                                "v0.2.4 mount refused startup on format-7 volume; local redb intact (sha_unchanged={})",
                                v024_b_sha_before == v024_b_sha_after
                            ),
                        );
                    }
                    true => {
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
                        if !v024_b_db.is_file() || fs::metadata(&v024_b_db)?.len() == 0 {
                            return Err(io::Error::other(
                                "v0.2.4 local redb was lost or emptied after fenced unmount",
                            )
                            .into());
                        }
                        record_op(
                            ops,
                            "v024_mount_fenced_and_local_redb_preserved",
                            CellStatus::Passed,
                            format!(
                                "v0.2.4 format-7 fence observed ({write_result}); local redb preserved"
                            ),
                        );
                    }
                }

                assert_shared_state_unchanged(&v024_config, &before_refusals, "v0.2.4 refusals")?;
                verify_cold_files(
                    &mut controller,
                    "client-v025-cold",
                    &[("from-v024.txt", v024_bytes), ("from-025.txt", v025_bytes)],
                )?;
                record_op(
                    ops,
                    "pg_s3_and_025_mount_intact_after_v024_fencing",
                    CellStatus::Passed,
                    "PG state unchanged and fresh 0.2.5 client read both S3 payloads",
                );

                Ok("published v0.2.4 failed closed after 0.2.5 format 6 -> 7 upgrade without corrupting PG/S3 or local redb".to_string())
            })();

            let _ = controller.unmount_all();
            cell_result
        },
    );

    pair
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
    let mount = &controller.client_by_name(name)?.mount_point;
    for (path, expected) in files {
        if fs::read(mount.join(path))? != *expected {
            return Err(
                io::Error::other(format!("cold client readback mismatch for {path}")).into(),
            );
        }
    }
    controller.unmount(name)?;
    Ok(())
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
    mut observe: impl FnMut() -> Result<bool, ScenarioError>,
) -> Result<(), ScenarioError> {
    let deadline = Instant::now() + timeout;
    loop {
        if observe()? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for {description}"),
            )
            .into());
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_runtime_fence(
    controller: &MultiClientController<SystemClientProcessExecutor>,
    name: &str,
) -> Result<(), ScenarioError> {
    wait_for_matrix_state(
        "baseline degraded with an upgrade-required sync error",
        Duration::from_secs(20),
        || {
            Ok(controller.runtime_snapshot(name).is_ok_and(|snapshot| {
                let sync = &snapshot["synchronization"]["state"]["pg_sync"];
                sync["lifecycle"] == "degraded"
                    && sync["write_authority"] == false
                    && sync["last_sync_error"] == "upgrade_required"
            }))
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
        || {
            if controller.runtime_snapshot(name).is_ok_and(|snapshot| {
                let sync = &snapshot["synchronization"]["state"]["pg_sync"];
                let format = &sync["last_sync_error"]["unsupported_format"];
                sync["lifecycle"] == "degraded"
                    && sync["write_authority"] == false
                    && format["found"] == 7
                    && format["expected"] == 6
            }) {
                return Ok(true);
            }
            for path in [&client.stderr_log, &client.stdout_log, &client.tracing_log] {
                match fs::read_to_string(path) {
                    Ok(log) if log.contains(FORMAT_7_REFUSAL) => return Ok(true),
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
}

fn observe_shared_state(config: &ClientControllerConfig) -> Result<SharedState, ScenarioError> {
    let volume_name = config.volume_name.clone();
    with_pg(config, "observe shared state", |client| async move {
        let row = client
            .query_one(
                "SELECT m.version, (SELECT count(*) FROM event_log WHERE vol = m.vol),
                    m.metadata_format_version, p.policy_epoch
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
        })
    })
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

fn wait_for_published_write(
    controller: &MultiClientController<SystemClientProcessExecutor>,
    writer: &str,
    config: &ClientControllerConfig,
    version: i64,
) -> Result<(), ScenarioError> {
    wait_for_matrix_state(
        "compatible client publication and an advanced metadata_state.version",
        CONVERGENCE_TIMEOUT,
        || {
            let runtime = controller.runtime_snapshot(writer)?;
            Ok(runtime["sync_safety"]["state"] == "safe"
                && runtime["upload"]["pending_uploads"] == 0
                && runtime["upload"]["active_uploads"] == 0
                && observe_shared_state(config)?.version > version)
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
    move |error| io::Error::other(format!("{op}: {error}")).into()
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
) -> Result<ObservedProtocolState, ScenarioError> {
    let deadline = Instant::now() + timeout;
    loop {
        let proto = observe_protocol_state(config)?;
        let has_f4 = proto.checkpoint_formats_observed.contains(&4);
        let has_f5 = proto.checkpoint_formats_observed.contains(&5);
        let pruned = proto.event_log_prune_floor.unwrap_or(0) > 0;
        if proto.checkpoint_version.unwrap_or(0) > 0 && has_f4 && has_f5 && pruned {
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
            let _ = child.wait();
            return Err(error.into());
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
    let _ = Command::new(bin)
        .args(["umount", &mount.display().to_string()])
        .env("STUDIOFS_CLIENT_ROOT", &client.client_root)
        .output();
    if !wait_for_child_exit(&mut proc.child, Duration::from_secs(10))? {
        let _ = proc.job.terminate();
        let _ = proc.job.force_kill();
        let _ = proc.child.kill();
        let _ = proc.child.wait();
    }
    Ok(())
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
    let SnapshotProc { mut child, job } = spawn_tracked_mount(&mut cmd)?;
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(ClientCommandOutput {
                status: status.code().unwrap_or(-1),
                stdout: String::from_utf8_lossy(&fs::read(stdout)?).into_owned(),
                stderr: String::from_utf8_lossy(&fs::read(stderr)?).into_owned(),
            });
        }
        if mount_point_is_active(&client.mount_point)? {
            let _ = Command::new(bin)
                .args(["umount", &client.mount_point.display().to_string()])
                .env("STUDIOFS_CLIENT_ROOT", &client.client_root)
                .output();
            let _ = job.terminate();
            let _ = job.force_kill();
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::other(format!(
                "expected mount for {} to fail closed, but mount point {} became active",
                client.name,
                client.mount_point.display()
            ))
            .into());
        }
        if Instant::now() >= deadline {
            let _ = job.terminate();
            let _ = job.force_kill();
            let _ = child.kill();
            let _ = child.wait();
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
        std::thread::sleep(POLL_INTERVAL);
    }
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
    if serde_json::to_string(report).is_ok_and(|json| contains_unredacted_diagnostics(&json)) {
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

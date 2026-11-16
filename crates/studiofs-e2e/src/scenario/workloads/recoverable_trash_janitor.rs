//! Mounted PostgreSQL + S3 janitor GC reclaim and protection proof for retained trash.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::process::{Command, Stdio};

use serde::{Deserialize, Serialize};
use studiofs_data::janitor::{JanitorKeyStat, JanitorObjectStore};
use studiofs_data::object::canonical_object_delete_keys;
use tokio_postgres::NoTls;

use super::recoverable_trash::{checked_command, expect_payload, wait_absent};
use super::snapshot_v2::{parse_snapshot_id, redact_diagnostics, snapshot_create_args};
use super::*;
use crate::client::ClientControllerConfig;

pub(in crate::scenario) struct RecoverableTrashJanitor;

const NAME: &str = "mounted-recoverable-trash-janitor-gc";
const CLIENT_NAMES: &[&str] = &["client-a", "client-b", "janitor"];
const LIVE_CLIENT_NAMES: &[&str] = &["client-a", "client-b"];
const LIMIT: Duration = Duration::from_secs(90);
const POLL: Duration = Duration::from_millis(200);
const DB_TIMEOUT: Duration = Duration::from_secs(15);

const PURGE_FILE: &str = "purge-me.bin";
const SNAP_FILE: &str = "snap-keep.bin";
const HARDLINK_TRASHED: &str = "hardlink-trashed.bin";
const OUTSIDE_LIVE: &str = "outside-live.bin";
const RESTORE_RACE_FILE: &str = "restore-race.bin";
const RESTORED_BEFORE_SETTLE: &str = "restored-before-settle.bin";

const PURGE_BYTES: &[u8] = b"janitor-gc-reclaim-purge-me\0\xff\n";
const SNAP_BYTES: &[u8] = b"janitor-gc-snapshot-protected\0\xfe\n";
const HARDLINK_BYTES: &[u8] = b"janitor-gc-hardlink-protected\0\xfd\n";
const RESTORE_RACE_BYTES: &[u8] = b"janitor-gc-restore-before-settle\0\xfc\n";

const FORBIDDEN_DAEMON_DELETION_MARKERS: &[&str] = &[
    "GC deleted object ",
    "GC deleted ",
    "janitor deleted ",
    "janitor: fragment delete failed",
    "janitor: fragment delete interrupted",
    "janitor: failed to claim object deletion",
    "janitor: failed to finish object deletion claim",
    "GC: failed to finish object deletion claim",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct JanitorCycleMetrics {
    pub(in crate::scenario) outcome: String,
    pub(in crate::scenario) coverage: String,
    pub(in crate::scenario) scanned: u64,
    pub(in crate::scenario) listed_entries: u64,
    pub(in crate::scenario) protected: u64,
    pub(in crate::scenario) orphan_candidates: u64,
    pub(in crate::scenario) skipped_young: u64,
    pub(in crate::scenario) skipped_unknown: u64,
    pub(in crate::scenario) delete_attempted: u64,
    pub(in crate::scenario) delete_confirmed: u64,
    pub(in crate::scenario) would_delete: u64,
    pub(in crate::scenario) delete_errors: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct PgObjectState {
    pub(in crate::scenario) object_id: u64,
    pub(in crate::scenario) current_ref_count: i64,
    pub(in crate::scenario) reachability_open_count: i64,
    pub(in crate::scenario) reachability_end_version: Option<i64>,
    pub(in crate::scenario) upload_state: Option<String>,
    pub(in crate::scenario) gc_candidate_present: bool,
    pub(in crate::scenario) gc_candidate_acknowledged: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct RecoverableTrashJanitorSummary {
    pub(in crate::scenario) passed: bool,
    pub(in crate::scenario) reclaim_verified: bool,
    pub(in crate::scenario) snapshot_protection_verified: bool,
    pub(in crate::scenario) snapshot_delete_reclaim_verified: bool,
    pub(in crate::scenario) hardlink_protection_verified: bool,
    pub(in crate::scenario) restore_before_settlement_verified: bool,
    pub(in crate::scenario) no_second_destructive_path_verified: bool,
    pub(in crate::scenario) snapshot_id: Option<String>,
    pub(in crate::scenario) snapshot_cut_version: Option<i64>,
    pub(in crate::scenario) data_prefix: String,
    pub(in crate::scenario) purge_object_id: Option<u64>,
    pub(in crate::scenario) snap_object_id: Option<u64>,
    pub(in crate::scenario) hardlink_object_id: Option<u64>,
    pub(in crate::scenario) restore_orig_object_id: Option<u64>,
    pub(in crate::scenario) restore_live_object_id: Option<u64>,
    pub(in crate::scenario) purge_keys: Vec<String>,
    pub(in crate::scenario) snap_keys: Vec<String>,
    pub(in crate::scenario) hardlink_keys: Vec<String>,
    pub(in crate::scenario) restore_orig_keys: Vec<String>,
    pub(in crate::scenario) restore_live_keys: Vec<String>,
    pub(in crate::scenario) s3_count_pre_janitor: usize,
    pub(in crate::scenario) s3_count_post_run1: usize,
    pub(in crate::scenario) s3_count_post_run2: usize,
    pub(in crate::scenario) dry_run1: Option<JanitorCycleMetrics>,
    pub(in crate::scenario) run1: Option<JanitorCycleMetrics>,
    pub(in crate::scenario) dry_run2: Option<JanitorCycleMetrics>,
    pub(in crate::scenario) run2: Option<JanitorCycleMetrics>,
    pub(in crate::scenario) daemon_log_files_scanned: usize,
    pub(in crate::scenario) daemon_deletion_log_hits: usize,
    pub(in crate::scenario) outside_hardlink_final_nlink: Option<u64>,
}

impl Scenario for RecoverableTrashJanitor {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: NAME,
            description: "Linux retained trash purge reclaim via studiofs janitor vs snapshot and live hardlink protection; peer restore before settlement retires the losing purge, keeps the restored copy durable, and reclaims the old payload only via janitor",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        if !cfg!(target_os = "linux") {
            return Err(
                io::Error::new(io::ErrorKind::Unsupported, "Linux GIO/FUSE required").into(),
            );
        }
        let config = context
            .client_controller_config
            .clone()
            .ok_or(ScenarioError::MissingComposeInfrastructure)?;
        let mut controller =
            MultiClientController::new(config.clone(), SystemClientProcessExecutor);
        let compose = compose_control_from_manifest(context)?;
        let mut postgres_paused = false;

        let result = probe(
            context,
            &config,
            &mut controller,
            &compose,
            &mut postgres_paused,
        );
        let mut cleanup_errors = Vec::new();
        if postgres_paused
            && let Err(error) = compose.start_service(POSTGRES_SERVICE_NAME, &SystemCommandExecutor)
        {
            cleanup_errors.push(format!("restart {POSTGRES_SERVICE_NAME}: {error}"));
        }
        let unmount = if matches!(
            result,
            Err(ScenarioError::MountedIoTimeout { .. } | ScenarioError::MountedIoWorkerLimit { .. })
        ) {
            controller.emergency_kill_all_no_wait("recoverable trash janitor timeout")
        } else {
            controller.unmount_all()
        };
        if let Err(error) = unmount {
            cleanup_errors.push(error.to_string());
        }
        match (result, cleanup_errors.is_empty()) {
            (Ok(()), true) => Ok(()),
            (Err(primary), true) => Err(primary),
            (result, false) => Err(io::Error::other(format!(
                "{}; cleanup: {}",
                result
                    .err()
                    .map_or_else(|| "probe passed".into(), |error| error.to_string()),
                cleanup_errors.join("; ")
            ))
            .into()),
        }
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary_path = context.scenario_dir.join("janitor-gc-summary.json");
        let summary: RecoverableTrashJanitorSummary =
            serde_json::from_slice(&fs::read(&summary_path)?)?;
        let valid = summary.passed
            && summary.reclaim_verified
            && summary.snapshot_protection_verified
            && summary.snapshot_delete_reclaim_verified
            && summary.hardlink_protection_verified
            && summary.restore_before_settlement_verified
            && summary.no_second_destructive_path_verified
            && summary.daemon_deletion_log_hits == 0
            && summary
                .run1
                .as_ref()
                .is_some_and(|c| c.delete_confirmed >= 1)
            && summary
                .run2
                .as_ref()
                .is_some_and(|c| c.delete_confirmed >= 1)
            && summary.s3_count_post_run1
                == summary
                    .s3_count_pre_janitor
                    .saturating_sub(summary.purge_keys.len() + summary.restore_orig_keys.len())
            && summary.s3_count_post_run2
                == summary
                    .s3_count_post_run1
                    .saturating_sub(summary.snap_keys.len())
            && !summary.purge_keys.is_empty()
            && !summary.restore_orig_keys.is_empty()
            && !summary.snap_keys.is_empty()
            && !summary.hardlink_keys.is_empty()
            && !summary.restore_live_keys.is_empty();
        if !valid {
            return Err(io::Error::other("janitor GC acceptance summary incomplete").into());
        }
        Ok(vec![format!(
            "artifacts/scenarios/{NAME}/janitor-gc-summary.json"
        )])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

#[cfg(unix)]
fn real_identity(path: &Path) -> io::Result<(u64, u64)> {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = fs::symlink_metadata(path)?;
    Ok((metadata.ino(), metadata.nlink()))
}

#[cfg(not(unix))]
fn real_identity(_path: &Path) -> io::Result<(u64, u64)> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Unix inode identity required",
    ))
}

fn save_json<T: Serialize>(
    context: &ScenarioContext,
    name: &str,
    value: &T,
) -> Result<(), ScenarioError> {
    fs::write(
        context.scenario_dir.join(name),
        serde_json::to_vec_pretty(value)?,
    )?;
    Ok(())
}

fn write_synced_file(mount: &Path, name: &str, bytes: &[u8]) -> Result<(), ScenarioError> {
    let path = mount.join(name);
    let bytes = bytes.to_vec();
    run_bounded_mounted_io(format!("write synced {name}"), LIMIT, move || {
        let mut file = fs::File::create(path)?;
        file.write_all(&bytes)?;
        file.sync_all()
    })
}

fn gio_trash_path(xdg: &Path, target: &Path) -> Result<(), ScenarioError> {
    fs::create_dir_all(xdg.join("data"))?;
    let xdg = xdg.to_path_buf();
    let target = target.to_path_buf();
    run_bounded_mounted_io("GIO trash entry", LIMIT, move || {
        let mut gio = Command::new("timeout");
        gio.args(["30s", "gio", "trash"])
            .arg(&target)
            .env("HOME", &xdg)
            .env("XDG_DATA_HOME", xdg.join("data"))
            .env("XDG_CONFIG_HOME", xdg.join("config"))
            .env("XDG_CACHE_HOME", xdg.join("cache"));
        checked_command(gio, "GIO trash")
    })
}

fn wait_client_pending_ops(
    controller: &MultiClientController<SystemClientProcessExecutor>,
    client: &str,
    baseline_gen: u64,
) -> Result<serde_json::Value, ScenarioError> {
    let deadline = Instant::now() + LIMIT;
    loop {
        let snapshot = controller.runtime_snapshot(client)?;
        let state = &snapshot["synchronization"]["state"];
        let pending = state["pending_metadata_ops"].as_u64().unwrap_or(0)
            + state["inflight_metadata_ops"].as_u64().unwrap_or(0);
        if pending > 0
            && snapshot["sync_safety"]["generation"]
                .as_u64()
                .is_some_and(|gen_val| gen_val > baseline_gen)
        {
            return Ok(snapshot);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for pending metadata ops on {client}"),
            )
            .into());
        }
        thread::sleep(POLL);
    }
}

fn wait_client_drained(
    controller: &MultiClientController<SystemClientProcessExecutor>,
    client: &str,
) -> Result<(), ScenarioError> {
    let deadline = Instant::now() + LIMIT;
    loop {
        let snapshot = controller.runtime_snapshot(client)?;
        let state = &snapshot["synchronization"]["state"];
        let pending = state["pending_metadata_ops"].as_u64().unwrap_or(0)
            + state["inflight_metadata_ops"].as_u64().unwrap_or(0);
        if pending == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("client {client} metadata journal did not drain: pending={pending}"),
            )
            .into());
        }
        thread::sleep(POLL);
    }
}

fn run_studiofs_cli<I, S>(
    config: &ClientControllerConfig,
    client_root: &Path,
    context: &ScenarioContext,
    action: &str,
    args: I,
) -> Result<(i32, String, String), ScenarioError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut cmd = Command::new("timeout");
    cmd.arg("60s")
        .arg(&config.studiofs_bin)
        .args(args)
        .env("STUDIOFS_CLIENT_ROOT", client_root)
        .env("STUDIOFS_DAEMON_LOG_DIR", client_root.join("logs"))
        .env("STUDIOFS_DAEMON_LOG_STEM", "studiofs-cli-tool")
        .stdin(Stdio::null());
    let output = cmd.output()?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    fs::write(
        context.scenario_dir.join(format!("{action}.stdout.log")),
        redact_diagnostics(config, &stdout),
    )?;
    fs::write(
        context.scenario_dir.join(format!("{action}.stderr.log")),
        redact_diagnostics(config, &stderr),
    )?;
    Ok((output.status.code().unwrap_or(-1), stdout, stderr))
}

fn run_studiofs_cli_ok<I, S>(
    config: &ClientControllerConfig,
    client_root: &Path,
    context: &ScenarioContext,
    action: &str,
    args: I,
) -> Result<String, ScenarioError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let (status, stdout, stderr) = run_studiofs_cli(config, client_root, context, action, args)?;
    if status != 0 {
        return Err(io::Error::other(format!(
            "{action} failed with exit {status}: {}",
            redact_diagnostics(config, &stderr)
        ))
        .into());
    }
    Ok(stdout)
}

pub(in crate::scenario) fn parse_janitor_jsonl(
    stdout: &str,
) -> Result<(JanitorCycleMetrics, serde_json::Value), io::Error> {
    let mut cycle: Option<JanitorCycleMetrics> = None;
    let mut summary: Option<serde_json::Value> = None;
    for line in stdout.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        match value.get("event").and_then(|v| v.as_str()) {
            Some("cycle") => {
                cycle = Some(JanitorCycleMetrics {
                    outcome: value["outcome"].as_str().unwrap_or("unknown").to_string(),
                    coverage: value["coverage"].as_str().unwrap_or("unknown").to_string(),
                    scanned: value["scanned"].as_u64().unwrap_or(0),
                    listed_entries: value["listed_entries"].as_u64().unwrap_or(0),
                    protected: value["protected"].as_u64().unwrap_or(0),
                    orphan_candidates: value["orphan_candidates"].as_u64().unwrap_or(0),
                    skipped_young: value["skipped_young"].as_u64().unwrap_or(0),
                    skipped_unknown: value["skipped_unknown"].as_u64().unwrap_or(0),
                    delete_attempted: value["delete_attempted"].as_u64().unwrap_or(0),
                    delete_confirmed: value["delete_confirmed"].as_u64().unwrap_or(0),
                    would_delete: value["would_delete"].as_u64().unwrap_or(0),
                    delete_errors: value["delete_errors"].as_u64().unwrap_or(0),
                });
            }
            Some("summary") => summary = Some(value),
            _ => {}
        }
    }
    match (cycle, summary) {
        (Some(cycle), Some(summary)) => Ok((cycle, summary)),
        _ => Err(io::Error::other(
            "janitor JSONL output missing cycle or summary event",
        )),
    }
}

fn wait_janitor_dry_run_ready(
    config: &ClientControllerConfig,
    janitor_root: &Path,
    context: &ScenarioContext,
    action: &str,
    min_would_delete: u64,
) -> Result<JanitorCycleMetrics, ScenarioError> {
    let deadline = Instant::now() + LIMIT;
    let mut attempt = 0_u32;
    loop {
        attempt += 1;
        let stdout = run_studiofs_cli_ok(
            config,
            janitor_root,
            context,
            &format!("{action}-attempt-{attempt}"),
            [
                "janitor",
                config.volume_name.as_str(),
                "--dry-run",
                "--full-scan",
                "--skip-newer-minutes",
                "0",
                "--output",
                "jsonl",
            ],
        )?;
        let (cycle, summary) = parse_janitor_jsonl(&stdout)?;
        if summary["exit_code"].as_i64() == Some(0)
            && cycle.outcome == "complete"
            && cycle.coverage == "complete"
            && cycle.skipped_young == 0
            && cycle.skipped_unknown == 0
            && cycle.would_delete >= min_would_delete
        {
            fs::write(
                context.scenario_dir.join(format!("{action}.final.jsonl")),
                redact_diagnostics(config, &stdout),
            )?;
            return Ok(cycle);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("{action} never became eligible with zero young/unknown skips: {cycle:?}"),
            )
            .into());
        }
        thread::sleep(POLL);
    }
}

fn run_janitor_reclaim(
    config: &ClientControllerConfig,
    janitor_root: &Path,
    context: &ScenarioContext,
    action: &str,
) -> Result<JanitorCycleMetrics, ScenarioError> {
    let stdout = run_studiofs_cli_ok(
        config,
        janitor_root,
        context,
        action,
        [
            "janitor",
            config.volume_name.as_str(),
            "--full-scan",
            "--skip-newer-minutes",
            "0",
            "--max-deletes",
            "500",
            "--output",
            "jsonl",
        ],
    )?;
    let (cycle, summary) = parse_janitor_jsonl(&stdout)?;
    if summary["exit_code"].as_i64() != Some(0)
        || summary["outcome"].as_str() != Some("complete")
        || cycle.outcome != "complete"
        || cycle.coverage != "complete"
        || cycle.delete_errors != 0
    {
        return Err(io::Error::other(format!(
            "{action} did not complete cleanly: cycle={cycle:?}, summary={summary}"
        ))
        .into());
    }
    Ok(cycle)
}

fn with_pg<F, Fut, T>(
    config: &ClientControllerConfig,
    op_name: &str,
    work: F,
) -> Result<T, ScenarioError>
where
    F: FnOnce(tokio_postgres::Client) -> Fut,
    Fut: std::future::Future<Output = Result<T, ScenarioError>>,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let (client, conn) =
            tokio::time::timeout(DB_TIMEOUT, tokio_postgres::connect(&config.meta_dsn, NoTls))
                .await
                .map_err(|_| io::Error::other(format!("{op_name}: PG connect timeout")))?
                .map_err(|err| {
                    io::Error::other(format!(
                        "{op_name}: PG connect failed: {}",
                        redact_diagnostics(config, &err.to_string())
                    ))
                })?;
        let conn_task = tokio::spawn(async move {
            let _ = conn.await;
        });
        let result = work(client).await;
        conn_task.abort();
        let _ = conn_task.await;
        result
    })
}

fn load_volume_id_and_prefix(
    config: &ClientControllerConfig,
) -> Result<(i64, String), ScenarioError> {
    with_pg(config, "load volume and prefix", |client| {
        let volume_name = config.volume_name.clone();
        async move {
            let row = tokio::time::timeout(
                DB_TIMEOUT,
                client.query_one(
                    "SELECT ms.vol, ms.format_info FROM metadata_state ms JOIN volumes v ON v.vol = ms.vol WHERE v.name = $1",
                    &[&volume_name],
                ),
            )
            .await
            .map_err(|_| io::Error::other("format_info query timeout"))?
            .map_err(|e| io::Error::other(format!("format_info query failed: {e}")))?;
            let vol: i64 = row.get(0);
            let bytes: Vec<u8> = row.get(1);
            let format_info = studiofs_meta::decode_format_info(&bytes)
                .map_err(|e| io::Error::other(format!("decode_format_info failed: {e}")))?;
            let raw = if format_info.data_prefix.trim().is_empty() {
                format!("{}/objects/", format_info.name)
            } else {
                format_info.data_prefix.clone()
            };
            let prefix = if raw.ends_with('/') {
                raw
            } else {
                format!("{raw}/")
            };
            Ok((vol, prefix))
        }
    })
}

fn query_pg_object_state(
    config: &ClientControllerConfig,
    vol: i64,
    object_id: u64,
) -> Result<PgObjectState, ScenarioError> {
    let obj_i64 =
        i64::try_from(object_id).map_err(|_| io::Error::other("object_id exceeds i64 range"))?;
    with_pg(config, "query pg object state", |client| async move {
        let row = tokio::time::timeout(
            DB_TIMEOUT,
            client.query_one(
                "SELECT
                    (SELECT count(*) FROM snapshot_current_references WHERE vol = $1 AND object_id = $2),
                    (SELECT count(*) FROM snapshot_object_reachability WHERE vol = $1 AND object_id = $2 AND end_version IS NULL),
                    (SELECT max(end_version) FROM snapshot_object_reachability WHERE vol = $1 AND object_id = $2),
                    (SELECT state FROM snapshot_payload_upload_objects WHERE vol = $1 AND object_id = $2),
                    (SELECT EXISTS (SELECT 1 FROM snapshot_payload_gc_candidates WHERE vol = $1 AND object_id = $2)),
                    (SELECT acknowledged_at IS NOT NULL FROM snapshot_payload_gc_candidates WHERE vol = $1 AND object_id = $2)",
                &[&vol, &obj_i64],
            ),
        )
        .await
        .map_err(|_| io::Error::other("object state query timeout"))?
        .map_err(|e| io::Error::other(format!("object state query failed: {e}")))?;
        Ok(PgObjectState {
            object_id,
            current_ref_count: row.get(0),
            reachability_open_count: row.get(1),
            reachability_end_version: row.get(2),
            upload_state: row.get(3),
            gc_candidate_present: row.get(4),
            gc_candidate_acknowledged: row.get(5),
        })
    })
}

fn wait_published_object_for_inode(
    config: &ClientControllerConfig,
    vol: i64,
    ino: u64,
    label: &str,
) -> Result<u64, ScenarioError> {
    wait_published_object_for_inode_impl(config, vol, ino, None, label)
}

fn wait_published_object_for_inode_excluding(
    config: &ClientControllerConfig,
    vol: i64,
    ino: u64,
    excluded_object_id: u64,
    label: &str,
) -> Result<u64, ScenarioError> {
    wait_published_object_for_inode_impl(config, vol, ino, Some(excluded_object_id), label)
}

fn wait_published_object_for_inode_impl(
    config: &ClientControllerConfig,
    vol: i64,
    ino: u64,
    excluded_object_id: Option<u64>,
    label: &str,
) -> Result<u64, ScenarioError> {
    let ino_i64 = i64::try_from(ino).map_err(|_| io::Error::other("inode exceeds i64 range"))?;
    let excluded_i64 = excluded_object_id
        .map(|id| i64::try_from(id).map_err(|_| io::Error::other("object_id exceeds i64 range")))
        .transpose()?;
    let deadline = Instant::now() + LIMIT;
    loop {
        let found = with_pg(
            config,
            "lookup inode published object",
            |client| async move {
                let row = tokio::time::timeout(
                    DB_TIMEOUT,
                    client.query_opt(
                        "SELECT r.object_id
                     FROM snapshot_current_references r
                     JOIN snapshot_payload_upload_objects u
                       ON u.vol = r.vol AND u.object_id = r.object_id
                     WHERE r.vol = $1
                       AND r.ino = $2
                       AND u.state = 'published'
                       AND ($3::BIGINT IS NULL OR r.object_id <> $3)
                     ORDER BY r.object_id
                     LIMIT 1",
                        &[&vol, &ino_i64, &excluded_i64],
                    ),
                )
                .await
                .map_err(|_| io::Error::other("published object lookup timeout"))?
                .map_err(|e| io::Error::other(format!("published object lookup failed: {e}")))?;
                Ok(row.map(|r| r.get::<_, i64>(0) as u64))
            },
        )?;
        if let Some(object_id) = found {
            return Ok(object_id);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("{label} inode {ino} did not publish to PG within limit"),
            )
            .into());
        }
        thread::sleep(POLL);
    }
}

fn wait_pg_object_state<F>(
    config: &ClientControllerConfig,
    vol: i64,
    object_id: u64,
    label: &str,
    predicate: F,
) -> Result<PgObjectState, ScenarioError>
where
    F: Fn(&PgObjectState) -> bool,
{
    let deadline = Instant::now() + LIMIT;
    loop {
        let state = query_pg_object_state(config, vol, object_id)?;
        if predicate(&state) {
            return Ok(state);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} object {object_id} did not reach expected PG state; last={state:?}"
                ),
            )
            .into());
        }
        thread::sleep(POLL);
    }
}

fn wait_snapshot_lifecycle(
    config: &ClientControllerConfig,
    vol: i64,
    snapshot_id: &str,
    expected: &[&str],
) -> Result<(String, i64), ScenarioError> {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let id = snapshot_id.to_string();
        let row = with_pg(config, "snapshot lifecycle poll", |client| async move {
            let row = tokio::time::timeout(
                DB_TIMEOUT,
                client.query_opt(
                    "SELECT lifecycle, cut_version FROM snapshot_catalog WHERE vol = $1 AND id = $2",
                    &[&vol, &id],
                ),
            )
            .await
            .map_err(|_| io::Error::other("snapshot lifecycle poll timeout"))?
            .map_err(|e| io::Error::other(format!("snapshot lifecycle poll failed: {e}")))?;
            Ok(row.map(|r| (r.get::<_, String>(0), r.get::<_, i64>(1))))
        })?;
        if let Some((lifecycle, cut_version)) = row
            && expected.iter().any(|target| *target == lifecycle)
        {
            return Ok((lifecycle, cut_version));
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("snapshot {snapshot_id} did not reach {expected:?}"),
            )
            .into());
        }
        thread::sleep(POLL);
    }
}

fn s3_store(config: &ClientControllerConfig) -> Result<JanitorObjectStore, ScenarioError> {
    JanitorObjectStore::from_s3(
        &config.s3.endpoint,
        &config.s3.bucket,
        &config.s3.access_key,
        &config.s3.secret_key,
        &config.s3.region,
        8,
    )
    .map_err(|e| io::Error::other(format!("init S3 store failed: {e}")).into())
}

type S3ObjectKeyListing = (BTreeSet<String>, BTreeMap<u64, BTreeSet<String>>);

fn list_s3_keys_by_object(
    config: &ClientControllerConfig,
    prefix: &str,
) -> Result<S3ObjectKeyListing, ScenarioError> {
    let store = s3_store(config)?;
    let prefix = prefix.to_string();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let mut lister = store
            .list(&prefix)
            .await
            .map_err(|e| io::Error::other(format!("S3 list failed: {e}")))?;
        let mut payload_keys = BTreeSet::new();
        let mut by_object: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
        while let Some(entry) = lister
            .next()
            .await
            .map_err(|e| io::Error::other(format!("S3 list page failed: {e}")))?
        {
            if !entry.is_file {
                continue;
            }
            if let Some((object_id, _)) = canonical_object_delete_keys(&prefix, &entry.key) {
                payload_keys.insert(entry.key.clone());
                by_object.entry(object_id).or_default().insert(entry.key);
            } else if !entry
                .key
                .strip_prefix(&prefix)
                .is_some_and(|rel| rel.starts_with("metadata/snapshots/"))
            {
                return Err(io::Error::other(format!(
                    "unexpected non-payload S3 key under {prefix}: {}",
                    entry.key
                ))
                .into());
            }
        }
        Ok((payload_keys, by_object))
    })
}

fn wait_s3_keys_for_object(
    config: &ClientControllerConfig,
    prefix: &str,
    object_id: u64,
    label: &str,
) -> Result<BTreeSet<String>, ScenarioError> {
    let deadline = Instant::now() + LIMIT;
    loop {
        let (_, by_object) = list_s3_keys_by_object(config, prefix)?;
        if let Some(keys) = by_object.get(&object_id)
            && !keys.is_empty()
        {
            return Ok(keys.clone());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("{label} object {object_id} keys absent from S3 under {prefix}"),
            )
            .into());
        }
        thread::sleep(POLL);
    }
}

fn assert_s3_keys_missing(
    config: &ClientControllerConfig,
    keys: &BTreeSet<String>,
    label: &str,
) -> Result<(), ScenarioError> {
    let store = s3_store(config)?;
    let keys = keys.clone();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        for key in keys {
            let stat = store
                .stat_key(&key)
                .await
                .map_err(|e| io::Error::other(format!("S3 stat failed on {key}: {e}")))?;
            if !matches!(stat, JanitorKeyStat::Missing) {
                return Err(io::Error::other(format!(
                    "{label}: S3 key still present after janitor reclaim: {key}"
                ))
                .into());
            }
        }
        Ok(())
    })
}

fn is_settled_post_janitor(state: &PgObjectState) -> bool {
    let deleted_fence = matches!(
        state.upload_state.as_deref(),
        Some("delete_pending" | "deleted")
    ) && state.gc_candidate_acknowledged == Some(true);
    let pruned_fence = state.upload_state.is_none() && !state.gc_candidate_present;
    state.current_ref_count == 0
        && state.reachability_open_count == 0
        && state.reachability_end_version.is_some()
        && (deleted_fence || pruned_fence)
}

pub(in crate::scenario) fn scan_log_contents_for_deletions(contents: &str) -> usize {
    contents
        .lines()
        .filter(|line| {
            FORBIDDEN_DAEMON_DELETION_MARKERS
                .iter()
                .any(|marker| line.contains(marker))
        })
        .count()
}

fn scan_client_daemon_logs_for_deletions(
    clients: &[crate::client::ClientSpec],
) -> Result<(usize, usize), ScenarioError> {
    let mut files = BTreeSet::new();
    for client in clients {
        for path in [&client.stdout_log, &client.stderr_log, &client.tracing_log] {
            if path.is_file() {
                files.insert(path.clone());
            }
        }
        // Rolled tracing files (`studiofs-tracing.log.<date>`) live beside the base file in the
        // per-client artifact dir; the janitor CLI's own logs live under client-root/logs.
        if let Some(parent) = client.tracing_log.parent()
            && let Some(base) = client.tracing_log.file_name().and_then(|n| n.to_str())
            && parent.is_dir()
        {
            for entry in fs::read_dir(parent)? {
                let path = entry?.path();
                if path.is_file()
                    && path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with(base))
                {
                    files.insert(path);
                }
            }
        }
    }
    let mut hits = 0usize;
    for file in &files {
        hits += scan_log_contents_for_deletions(&String::from_utf8_lossy(&fs::read(file)?));
    }
    Ok((files.len(), hits))
}

fn probe(
    context: &ScenarioContext,
    config: &ClientControllerConfig,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
    compose: &ComposeServiceControl,
    postgres_paused: &mut bool,
) -> Result<(), ScenarioError> {
    let mut summary = RecoverableTrashJanitorSummary::default();

    controller.bootstrap_volume(VolumeBootstrapOptions {
        client_names: CLIENT_NAMES.iter().map(|s| (*s).to_string()).collect(),
        clean_existing_local_metadata: true,
    })?;
    let a = controller.client_at(0)?.clone();
    let b = controller.client_at(1)?.clone();
    let janitor_client = controller.client_at(2)?.clone();

    run_studiofs_cli_ok(
        config,
        &a.client_root,
        context,
        "enable-trash-days",
        ["trash-days", &config.volume_name, "7"],
    )?;

    for client_name in LIVE_CLIENT_NAMES {
        controller.mount(client_name)?;
    }
    controller.mount("janitor")?;
    let prime_deadline = Instant::now() + LIMIT;
    loop {
        let snapshot = controller.runtime_snapshot("janitor")?;
        let state = &snapshot["synchronization"]["state"];
        let pending = state["pending_metadata_ops"].as_u64().unwrap_or(0)
            + state["inflight_metadata_ops"].as_u64().unwrap_or(0);
        if pending == 0 && snapshot["sync_safety"]["state"].as_str() == Some("safe") {
            break;
        }
        if Instant::now() >= prime_deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("janitor client did not reach safe sync state: {snapshot}"),
            )
            .into());
        }
        thread::sleep(POLL);
    }
    controller.unmount("janitor")?;

    let uid_output = Command::new("id").arg("-u").output()?;
    if !uid_output.status.success() {
        return Err(io::Error::other("could not resolve mount uid").into());
    }
    let trash = format!(
        ".Trash-{}",
        String::from_utf8_lossy(&uid_output.stdout).trim()
    );
    let (vol, prefix) = load_volume_id_and_prefix(config)?;
    summary.data_prefix = prefix.clone();

    // 1. Write snap-keep and the hardlink pair before taking the snapshot.
    write_synced_file(&a.mount_point, SNAP_FILE, SNAP_BYTES)?;
    write_synced_file(&a.mount_point, HARDLINK_TRASHED, HARDLINK_BYTES)?;
    let hardlink_from = a.mount_point.join(HARDLINK_TRASHED);
    let hardlink_to = a.mount_point.join(OUTSIDE_LIVE);
    run_bounded_mounted_io("create outside hardlink", LIMIT, move || {
        fs::hard_link(hardlink_from, hardlink_to)
    })?;

    let (snap_ino, _) = real_identity(&a.mount_point.join(SNAP_FILE))?;
    let (hardlink_ino, hl_nlink) = real_identity(&a.mount_point.join(HARDLINK_TRASHED))?;
    let outside_id = real_identity(&a.mount_point.join(OUTSIDE_LIVE))?;
    if hl_nlink != 2 || outside_id != (hardlink_ino, 2) {
        return Err(io::Error::other("hardlink identity mismatch before trash").into());
    }

    // Observe peer namespace metadata only (do not read OUTSIDE_LIVE payload on client-b yet).
    let peer_mount = b.mount_point.clone();
    run_bounded_mounted_io("peer initial convergence", LIMIT, move || {
        let deadline = Instant::now() + Duration::from_secs(80);
        while !(peer_mount.join(SNAP_FILE).is_file()
            && peer_mount.join(HARDLINK_TRASHED).is_file()
            && real_identity(&peer_mount.join(OUTSIDE_LIVE))
                .is_ok_and(|id| id == (hardlink_ino, 2)))
        {
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "peer did not observe initial files",
                ));
            }
            thread::sleep(POLL);
        }
        Ok(())
    })?;

    let snap_obj = wait_published_object_for_inode(config, vol, snap_ino, SNAP_FILE)?;
    let hardlink_obj =
        wait_published_object_for_inode(config, vol, hardlink_ino, HARDLINK_TRASHED)?;

    let snap_keys = wait_s3_keys_for_object(config, &prefix, snap_obj, SNAP_FILE)?;
    let hardlink_keys = wait_s3_keys_for_object(config, &prefix, hardlink_obj, HARDLINK_TRASHED)?;

    summary.snap_object_id = Some(snap_obj);
    summary.hardlink_object_id = Some(hardlink_obj);
    summary.snap_keys = snap_keys.iter().cloned().collect();
    summary.hardlink_keys = hardlink_keys.iter().cloned().collect();

    // 2. Protection (a): Trash snap-keep.bin and capture a ready snapshot BEFORE purge.
    let xdg = context.scenario_dir.join("isolated-xdg");
    gio_trash_path(&xdg, &a.mount_point.join(SNAP_FILE))?;
    let b_files = b.mount_point.join(&trash).join("files");
    let b_info = b.mount_point.join(&trash).join("info");
    let wait_b_files = b_files.clone();
    let wait_b_info = b_info.clone();
    run_bounded_mounted_io("peer observes retained snap-keep", LIMIT, move || {
        let deadline = Instant::now() + Duration::from_secs(80);
        while !(wait_b_files.join(SNAP_FILE).is_file()
            && wait_b_info.join(format!("{SNAP_FILE}.trashinfo")).is_file())
        {
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "peer did not observe trashed snap-keep.bin",
                ));
            }
            thread::sleep(POLL);
        }
        Ok(())
    })?;

    let snap_out = run_studiofs_cli_ok(
        config,
        &a.client_root,
        context,
        "snapshot-create",
        snapshot_create_args(&config.volume_name),
    )?;
    let snapshot_id = parse_snapshot_id(&snap_out)
        .ok_or_else(|| io::Error::other("snapshot create output missing snapshot_id"))?;
    let (_, cut_version) = wait_snapshot_lifecycle(config, vol, &snapshot_id, &["ready"])?;
    summary.snapshot_id = Some(snapshot_id.clone());
    summary.snapshot_cut_version = Some(cut_version);
    eprintln!("janitor gc: snapshot {snapshot_id} ready at cut_version={cut_version}");

    // 3. Create purge-me.bin and restore-race.bin AFTER the snapshot cut so the snapshot never
    //    pins either payload, then trash the rest.
    write_synced_file(&a.mount_point, PURGE_FILE, PURGE_BYTES)?;
    write_synced_file(&a.mount_point, RESTORE_RACE_FILE, RESTORE_RACE_BYTES)?;
    let (purge_ino, _) = real_identity(&a.mount_point.join(PURGE_FILE))?;
    let (restore_ino, _) = real_identity(&a.mount_point.join(RESTORE_RACE_FILE))?;
    let purge_obj = wait_published_object_for_inode(config, vol, purge_ino, PURGE_FILE)?;
    let restore_orig_obj =
        wait_published_object_for_inode(config, vol, restore_ino, RESTORE_RACE_FILE)?;
    let purge_keys = wait_s3_keys_for_object(config, &prefix, purge_obj, PURGE_FILE)?;
    let restore_orig_keys =
        wait_s3_keys_for_object(config, &prefix, restore_orig_obj, RESTORE_RACE_FILE)?;
    summary.purge_object_id = Some(purge_obj);
    summary.purge_keys = purge_keys.iter().cloned().collect();
    summary.restore_orig_object_id = Some(restore_orig_obj);
    summary.restore_orig_keys = restore_orig_keys.iter().cloned().collect();

    gio_trash_path(&xdg, &a.mount_point.join(PURGE_FILE))?;
    gio_trash_path(&xdg, &a.mount_point.join(HARDLINK_TRASHED))?;
    gio_trash_path(&xdg, &a.mount_point.join(RESTORE_RACE_FILE))?;

    let wait_b_files = b_files.clone();
    let wait_b_info = b_info.clone();
    run_bounded_mounted_io("peer observes retained trash receipts", LIMIT, move || {
        let deadline = Instant::now() + Duration::from_secs(80);
        while !(wait_b_files.join(PURGE_FILE).is_file()
            && wait_b_files.join(HARDLINK_TRASHED).is_file()
            && wait_b_files.join(RESTORE_RACE_FILE).is_file()
            && wait_b_info
                .join(format!("{PURGE_FILE}.trashinfo"))
                .is_file()
            && wait_b_info
                .join(format!("{HARDLINK_TRASHED}.trashinfo"))
                .is_file()
            && wait_b_info
                .join(format!("{RESTORE_RACE_FILE}.trashinfo"))
                .is_file())
        {
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "peer did not observe all trashed entries",
                ));
            }
            thread::sleep(POLL);
        }
        Ok(())
    })?;

    // 4. Protection (c): client-a purges restore-race.bin while PG is offline and crashes;
    //    client-b restores restore-race.bin before client-a replays its losing purge.
    let baseline_gen = controller.runtime_snapshot("client-a")?["sync_safety"]["generation"]
        .as_u64()
        .ok_or_else(|| io::Error::other("missing client-a generation"))?;
    *postgres_paused = true;
    compose.stop_service(POSTGRES_SERVICE_NAME, &SystemCommandExecutor)?;
    let a_trash_restore = a
        .mount_point
        .join(&trash)
        .join("files")
        .join(RESTORE_RACE_FILE);
    let a_barrier = a.mount_point.join("local-barrier.bin");
    run_bounded_mounted_io("offline purge on client-a", LIMIT, move || {
        fs::remove_file(a_trash_restore)?;
        fs::File::create(a_barrier)?.sync_all()
    })?;
    wait_client_pending_ops(controller, "client-a", baseline_gen)?;
    controller.force_kill("client-a")?;

    compose.start_service(POSTGRES_SERVICE_NAME, &SystemCommandExecutor)?;
    *postgres_paused = false;

    let b_restore_src = b_files.join(RESTORE_RACE_FILE);
    let b_restore_dst = b.mount_point.join(RESTORED_BEFORE_SETTLE);
    // Restore of a payload absent from the peer's local cache answers EAGAIN
    // (`RemoteContentPending`) after queueing a fetch. Wait until the retained source is
    // locally readable with the expected bytes, then restore exactly once.
    expect_payload(&b_restore_src, RESTORE_RACE_BYTES)?;
    run_bounded_mounted_io(
        "peer restores before offline purge settles",
        LIMIT,
        move || fs::rename(&b_restore_src, &b_restore_dst),
    )?;
    wait_absent(b_files.join(RESTORE_RACE_FILE))?;
    expect_payload(
        &b.mount_point.join(RESTORED_BEFORE_SETTLE),
        RESTORE_RACE_BYTES,
    )?;

    wait_client_drained(controller, "client-b")?;
    let (restore_live_ino, _) = real_identity(&b.mount_point.join(RESTORED_BEFORE_SETTLE))?;
    let restore_live_obj = wait_published_object_for_inode_excluding(
        config,
        vol,
        restore_live_ino,
        restore_orig_obj,
        RESTORED_BEFORE_SETTLE,
    )?;
    // Restore renames the retained inode back into the namespace and adopts a staged copy as
    // a fresh payload object, so the losing purge's old generation cannot reach it.
    if restore_live_ino != restore_ino || restore_live_obj == restore_orig_obj {
        return Err(io::Error::other(format!(
            "restore identity mismatch: ino {restore_ino}->{restore_live_ino}, object {restore_orig_obj}->{restore_live_obj}"
        ))
        .into());
    }
    let restore_live_keys =
        wait_s3_keys_for_object(config, &prefix, restore_live_obj, RESTORED_BEFORE_SETTLE)?;
    summary.restore_live_object_id = Some(restore_live_obj);
    summary.restore_live_keys = restore_live_keys.iter().cloned().collect();

    controller.remount("client-a")?;
    wait_client_drained(controller, "client-a")?;
    expect_payload(
        &a.mount_point.join(RESTORED_BEFORE_SETTLE),
        RESTORE_RACE_BYTES,
    )?;
    wait_absent(
        a.mount_point
            .join(&trash)
            .join("files")
            .join(RESTORE_RACE_FILE),
    )?;

    // 5. Purge purge-me, snap-keep, and hardlink-trashed, then restart client-b for immediate
    //    Live-transition retained purge settlement.
    let a_files = a.mount_point.join(&trash).join("files");
    run_bounded_mounted_io("purge trashed targets", LIMIT, move || {
        fs::remove_file(a_files.join(PURGE_FILE))?;
        fs::remove_file(a_files.join(SNAP_FILE))?;
        fs::remove_file(a_files.join(HARDLINK_TRASHED))
    })?;
    for name in [PURGE_FILE, SNAP_FILE, HARDLINK_TRASHED] {
        wait_absent(b_files.join(name))?;
        wait_absent(b_info.join(format!("{name}.trashinfo")))?;
    }
    controller.restart("client-b")?;

    let unacked_candidate = |s: &PgObjectState| {
        s.current_ref_count == 0
            && s.reachability_open_count == 0
            && s.reachability_end_version.is_some()
            && s.upload_state.as_deref() == Some("published")
            && s.gc_candidate_present
            && s.gc_candidate_acknowledged == Some(false)
    };
    wait_pg_object_state(config, vol, purge_obj, PURGE_FILE, unacked_candidate)?;
    wait_pg_object_state(config, vol, snap_obj, SNAP_FILE, unacked_candidate)?;
    // The restore commit superseded the old retained generation; no snapshot pins it.
    wait_pg_object_state(
        config,
        vol,
        restore_orig_obj,
        RESTORE_RACE_FILE,
        unacked_candidate,
    )?;
    let hl_settled = query_pg_object_state(config, vol, hardlink_obj)?;
    let restore_settled = query_pg_object_state(config, vol, restore_live_obj)?;
    if hl_settled.current_ref_count < 1
        || hl_settled.gc_candidate_present
        || restore_settled.current_ref_count < 1
        || restore_settled.gc_candidate_present
    {
        return Err(io::Error::other(format!(
            "protected object lost ref or entered GC queue prematurely: hl={hl_settled:?}, restore={restore_settled:?}"
        ))
        .into());
    }

    // 6. Prove no second destructive path before janitor runs: every key stays in S3 after settlement.
    let (pre_janitor_keys, _) = list_s3_keys_by_object(config, &prefix)?;
    for key in purge_keys
        .iter()
        .chain(snap_keys.iter())
        .chain(hardlink_keys.iter())
        .chain(restore_orig_keys.iter())
        .chain(restore_live_keys.iter())
    {
        if !pre_janitor_keys.contains(key) {
            return Err(io::Error::other(format!(
                "second destructive path: S3 key missing before janitor ran: {key}"
            ))
            .into());
        }
    }
    summary.s3_count_pre_janitor = pre_janitor_keys.len();

    // 7. Janitor Run 1: reclaim purge-me and the superseded pre-restore payload; protect
    //    snap-keep, outside-live, and restored-before-settle.
    let dry_run1 = wait_janitor_dry_run_ready(
        config,
        &janitor_client.client_root,
        context,
        "janitor-dry-run-1",
        2,
    )?;
    let run1 = run_janitor_reclaim(
        config,
        &janitor_client.client_root,
        context,
        "janitor-run-1",
    )?;
    summary.dry_run1 = Some(dry_run1);
    summary.run1 = Some(run1);

    let (post_run1_keys, _) = list_s3_keys_by_object(config, &prefix)?;
    summary.s3_count_post_run1 = post_run1_keys.len();
    assert_s3_keys_missing(config, &purge_keys, "post-run-1 purge-me")?;
    assert_s3_keys_missing(config, &restore_orig_keys, "post-run-1 pre-restore payload")?;
    let expected_run1_removed: BTreeSet<String> =
        purge_keys.union(&restore_orig_keys).cloned().collect();
    let removed_run1: BTreeSet<String> = pre_janitor_keys
        .difference(&post_run1_keys)
        .cloned()
        .collect();
    if removed_run1 != expected_run1_removed {
        return Err(io::Error::other(format!(
            "Run 1 removed unexpected key set: expected {expected_run1_removed:?}, got {removed_run1:?}"
        ))
        .into());
    }
    for key in snap_keys
        .iter()
        .chain(hardlink_keys.iter())
        .chain(restore_live_keys.iter())
    {
        if !post_run1_keys.contains(key) {
            return Err(io::Error::other(format!("Run 1 deleted protected S3 key: {key}")).into());
        }
    }
    let pg_purge_after_run1 = query_pg_object_state(config, vol, purge_obj)?;
    let pg_restore_orig_after_run1 = query_pg_object_state(config, vol, restore_orig_obj)?;
    let pg_snap_after_run1 = query_pg_object_state(config, vol, snap_obj)?;
    if !is_settled_post_janitor(&pg_purge_after_run1)
        || !is_settled_post_janitor(&pg_restore_orig_after_run1)
    {
        return Err(io::Error::other(format!(
            "reclaimed PG state not settled post-run-1: purge={pg_purge_after_run1:?}, pre-restore={pg_restore_orig_after_run1:?}"
        ))
        .into());
    }
    if pg_snap_after_run1.upload_state.as_deref() != Some("published")
        || pg_snap_after_run1.gc_candidate_acknowledged != Some(false)
    {
        return Err(io::Error::other(format!(
            "snapshot-pinned PG state modified during Run 1: {pg_snap_after_run1:?}"
        ))
        .into());
    }
    summary.reclaim_verified = true;
    summary.snapshot_protection_verified = true;

    // 8. Delete snapshot and run Janitor Run 2: prove snap-keep now reclaims while live hardlink
    //    and restored copy remain untouched.
    run_studiofs_cli_ok(
        config,
        &a.client_root,
        context,
        "snapshot-delete",
        ["snapshot", &config.volume_name, "delete", &snapshot_id],
    )?;
    wait_snapshot_lifecycle(config, vol, &snapshot_id, &["deleting", "deleted"])?;
    let dry_run2 = wait_janitor_dry_run_ready(
        config,
        &janitor_client.client_root,
        context,
        "janitor-dry-run-2",
        1,
    )?;
    let run2 = run_janitor_reclaim(
        config,
        &janitor_client.client_root,
        context,
        "janitor-run-2",
    )?;
    summary.dry_run2 = Some(dry_run2);
    summary.run2 = Some(run2);

    let (post_run2_keys, _) = list_s3_keys_by_object(config, &prefix)?;
    summary.s3_count_post_run2 = post_run2_keys.len();
    assert_s3_keys_missing(config, &snap_keys, "post-run-2 snap-keep")?;
    let removed_run2: BTreeSet<String> = post_run1_keys
        .difference(&post_run2_keys)
        .cloned()
        .collect();
    if removed_run2 != snap_keys {
        return Err(io::Error::other(format!(
            "Run 2 removed unexpected key set: expected {snap_keys:?}, got {removed_run2:?}"
        ))
        .into());
    }
    for key in hardlink_keys.iter().chain(restore_live_keys.iter()) {
        if !post_run2_keys.contains(key) {
            return Err(io::Error::other(format!("Run 2 deleted protected S3 key: {key}")).into());
        }
    }
    let pg_snap_after_run2 = query_pg_object_state(config, vol, snap_obj)?;
    if !is_settled_post_janitor(&pg_snap_after_run2) {
        return Err(io::Error::other(format!(
            "snap-keep PG state not settled post-run-2: {pg_snap_after_run2:?}"
        ))
        .into());
    }
    summary.snapshot_delete_reclaim_verified = true;

    // 9. Verify live hardlink and restored copy remain readable across clients after two janitor runs.
    controller.restart("client-b")?;
    let (final_hl_ino, final_hl_nlink) = real_identity(&b.mount_point.join(OUTSIDE_LIVE))?;
    if final_hl_ino != hardlink_ino || final_hl_nlink != 1 {
        return Err(io::Error::other(format!(
            "outside hardlink final identity mismatch: expected ({hardlink_ino}, 1), got ({final_hl_ino}, {final_hl_nlink})"
        ))
        .into());
    }
    summary.outside_hardlink_final_nlink = Some(final_hl_nlink);
    expect_payload(&a.mount_point.join(OUTSIDE_LIVE), HARDLINK_BYTES)?;
    expect_payload(&b.mount_point.join(OUTSIDE_LIVE), HARDLINK_BYTES)?;
    expect_payload(
        &a.mount_point.join(RESTORED_BEFORE_SETTLE),
        RESTORE_RACE_BYTES,
    )?;
    expect_payload(
        &b.mount_point.join(RESTORED_BEFORE_SETTLE),
        RESTORE_RACE_BYTES,
    )?;
    summary.hardlink_protection_verified = true;
    summary.restore_before_settlement_verified = true;

    // 10. Scan daemon logs for zero background deletion evidence.
    let (scanned_files, log_hits) = scan_client_daemon_logs_for_deletions(&[a, b, janitor_client])?;
    summary.daemon_log_files_scanned = scanned_files;
    summary.daemon_deletion_log_hits = log_hits;
    if log_hits != 0 {
        return Err(io::Error::other(format!(
            "detected {log_hits} daemon deletion log hits across {scanned_files} log files"
        ))
        .into());
    }
    summary.no_second_destructive_path_verified = true;
    summary.passed = true;

    save_json(context, "janitor-gc-summary.json", &summary)?;
    eprintln!(
        "janitor gc acceptance passed: pre={}, post_run1={}, post_run2={}, log_files={scanned_files}",
        summary.s3_count_pre_janitor, summary.s3_count_post_run1, summary.s3_count_post_run2
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn janitor_summary_round_trips_through_json() {
        let summary = RecoverableTrashJanitorSummary {
            passed: true,
            s3_count_pre_janitor: 4,
            s3_count_post_run1: 3,
            s3_count_post_run2: 2,
            ..Default::default()
        };
        let encoded = serde_json::to_vec(&summary).expect("serialize summary");
        let decoded: RecoverableTrashJanitorSummary =
            serde_json::from_slice(&encoded).expect("deserialize summary");
        assert_eq!(decoded, summary);
    }

    #[test]
    fn janitor_run_requires_compose_infrastructure() {
        let temp = studiofs_test_support::workspace_target_tempdir("janitor-run-");
        let context = ScenarioContext {
            run_id: "run".to_string(),
            seed: 0,
            sandbox: temp.path().join("sandbox"),
            scenario_dir: temp.path().join("scenario"),
            client_controller_config: None,
        };
        if cfg!(target_os = "linux") {
            assert!(matches!(
                RecoverableTrashJanitor.run(&context),
                Err(ScenarioError::MissingComposeInfrastructure)
            ));
        } else {
            assert!(RecoverableTrashJanitor.run(&context).is_err());
        }
    }

    #[test]
    fn janitor_validation_rejects_an_incomplete_summary() {
        let temp = studiofs_test_support::workspace_target_tempdir("janitor-validation-");
        let context = ScenarioContext {
            run_id: "validation".to_string(),
            seed: 0,
            sandbox: temp.path().join("sandbox"),
            scenario_dir: temp.path().join("scenario"),
            client_controller_config: None,
        };
        fs::create_dir_all(&context.scenario_dir).expect("create scenario dir");
        let summary = RecoverableTrashJanitorSummary::default();
        fs::write(
            context.scenario_dir.join("janitor-gc-summary.json"),
            serde_json::to_vec(&summary).expect("serialize summary"),
        )
        .expect("write summary");
        assert!(RecoverableTrashJanitor.validate(&context).is_err());
    }

    #[test]
    fn janitor_jsonl_parser_extracts_cycle_and_summary() {
        let jsonl = "\
{\"schema_version\":1,\"event\":\"progress\",\"phase\":\"object_scan\"}\n\
{\"schema_version\":1,\"event\":\"cycle\",\"outcome\":\"complete\",\"coverage\":\"complete\",\"scanned\":4,\"listed_entries\":4,\"protected\":3,\"orphan_candidates\":1,\"skipped_young\":0,\"skipped_unknown\":0,\"delete_attempted\":1,\"delete_confirmed\":1,\"would_delete\":0,\"delete_errors\":0}\n\
{\"schema_version\":1,\"event\":\"summary\",\"outcome\":\"complete\",\"coverage\":\"complete\",\"exit_code\":0}\n";
        let (cycle, summary) = parse_janitor_jsonl(jsonl).expect("parse jsonl");
        assert_eq!(cycle.outcome, "complete");
        assert_eq!(cycle.delete_confirmed, 1);
        assert_eq!(cycle.protected, 3);
        assert_eq!(summary["exit_code"], 0);
    }

    #[test]
    fn janitor_log_scanner_flags_forbidden_deletion_markers() {
        assert_eq!(
            scan_log_contents_for_deletions("INFO mounted cleanly\nDEBUG sync ok\n"),
            0
        );
        assert_eq!(
            scan_log_contents_for_deletions(
                "INFO GC deleted object 42\nWARN janitor: fragment delete failed\n"
            ),
            2
        );
    }
}

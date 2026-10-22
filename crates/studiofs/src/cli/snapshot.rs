//! `studiofs snapshot` — PostgreSQL-authoritative Snapshot v2 operations.

use anyhow::{Context, Result, anyhow};
use chrono::NaiveTime;
use clap::{ArgMatches, Command};
use clap::{Args, FromArgMatches, Subcommand};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

use studiofs_data::{DataStore, DataStoreConfig};
use studiofs_meta::snapshot::{
    SnapshotCatalogEntry, SnapshotDailyPolicy, SnapshotId, SnapshotLifecycle, SnapshotOrigin,
    SnapshotRetention, SnapshotVolumeContext, SnapshotVolumeIdentityHint,
};
use studiofs_meta::{FormatInfo, MetadataBackend, PgMetaEngine, PgSnapshotBackend, RedbMetaEngine};
use studiofs_vfs::{
    DataStoreSnapshotArtifactStore, DataStoreSnapshotPayloadReader, SnapshotWorker,
    SnapshotWorkerConfig, SystemSnapshotClock,
};

#[derive(Debug)]
pub(crate) struct SnapshotArgs {
    volume_name: String,
    command: SnapshotCommand,
    log_level_override: Option<String>,
}

#[derive(Debug, Args)]
struct SnapshotRawArgs {
    /// Name of the PostgreSQL-backed volume to operate on.
    #[arg(value_name = "VOLUME_NAME", allow_hyphen_values = true)]
    volume_name: String,
    #[arg(
        value_name = "COMMAND_ARGUMENT",
        num_args = 0..,
        allow_hyphen_values = true,
        trailing_var_arg = true
    )]
    command_arguments: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum SnapshotCommand {
    /// Create a manual snapshot and wait until its artifact is Ready.
    Create(CreateArgs),
    /// List snapshots for the named volume.
    List,
    /// Inspect one snapshot by its PostgreSQL-allocated identifier.
    Inspect(InspectArgs),
    /// Request idempotent deletion of one snapshot.
    Delete(DeleteArgs),
    /// Configure, inspect, or cancel the daily automatic snapshot policy.
    Policy(PolicyArgs),
}

impl clap::Args for SnapshotArgs {
    fn augment_args(command: Command) -> Command {
        <SnapshotRawArgs as clap::Args>::augment_args(command)
            .about("Create and recover whole-volume Snapshot v2 views")
            .override_usage("studiofs snapshot <VOLUME_NAME> <COMMAND>")
            .disable_help_flag(true)
            .override_help(snapshot_command_schema().render_help().to_string())
    }

    fn augment_args_for_update(command: Command) -> Command {
        Self::augment_args(command)
    }
}

impl FromArgMatches for SnapshotArgs {
    fn from_arg_matches(matches: &ArgMatches) -> std::result::Result<Self, clap::Error> {
        let raw = SnapshotRawArgs::from_arg_matches(matches)?;
        let mut command_arguments = raw.command_arguments;
        // Clap reserves first-token subcommand names; parse this command's tail separately.
        if command_arguments.is_empty() && matches!(raw.volume_name.as_str(), "-h" | "--help") {
            command_arguments.push(raw.volume_name.clone());
        }
        let matches = snapshot_command_schema().try_get_matches_from(
            std::iter::once("studiofs snapshot <VOLUME_NAME>")
                .chain(command_arguments.iter().map(String::as_str)),
        )?;
        let command = SnapshotCommand::from_arg_matches(&matches)?;
        Ok(Self {
            volume_name: raw.volume_name,
            command,
            log_level_override: matches.get_one::<String>("log_level").cloned(),
        })
    }

    fn update_from_arg_matches(
        &mut self,
        matches: &ArgMatches,
    ) -> std::result::Result<(), clap::Error> {
        *self = Self::from_arg_matches(matches)?;
        Ok(())
    }
}

fn snapshot_command_schema() -> Command {
    SnapshotCommand::augment_subcommands(
        Command::new("studiofs")
            .bin_name("studiofs snapshot <VOLUME_NAME>")
            .about("Create and recover whole-volume Snapshot v2 views for the named volume")
            .arg(
                clap::Arg::new("log_level")
                    .long("log-level")
                    .value_name("LOG_LEVEL")
                    .help("Log level (trace, debug, info, warn, error)")
                    .global(true),
            )
            .subcommand_required(true),
    )
}

#[derive(Debug, Args)]
struct CreateArgs {
    /// Retention beginning when the snapshot becomes Ready (for example 7d).
    #[arg(long, value_parser = parse_duration)]
    ttl: Option<Duration>,
}

#[derive(Debug, Args)]
struct InspectArgs {
    snapshot_id: SnapshotId,
}

#[derive(Debug, Args)]
struct DeleteArgs {
    snapshot_id: SnapshotId,
}

#[derive(Debug, Args)]
struct PolicyArgs {
    #[command(subcommand)]
    command: PolicyCommand,
}

#[derive(Debug, Subcommand)]
enum PolicyCommand {
    /// Enable or replace the one daily automatic snapshot policy.
    Set(PolicySetArgs),
    /// Show the active daily policy.
    Show,
    /// Cancel future automatic snapshot claims.
    Cancel,
}

#[derive(Debug, Args)]
struct PolicySetArgs {
    /// Local wall-clock schedule in the form HH:MM.
    #[arg(long, value_parser = parse_daily_at)]
    daily_at: NaiveTime,
    /// IANA timezone name. Defaults to UTC and is persisted explicitly.
    #[arg(long, value_parser = parse_timezone)]
    timezone: Option<String>,
    /// Automatic retention beginning when the snapshot becomes Ready.
    #[arg(long, value_parser = parse_duration)]
    retention: Duration,
}

impl SnapshotArgs {
    pub(super) fn log_level_override(&self) -> Option<&str> {
        self.log_level_override.as_deref()
    }

    pub(crate) fn dispatch(self) -> Result<()> {
        let client_root = crate::cli::utils::get_client_root()?;
        let volume = resolve_volume(&client_root, &self.volume_name)?;
        let runtime = runtime()?;
        match self.command {
            SnapshotCommand::Create(args) => create_snapshot(&volume, args.ttl, &runtime),
            SnapshotCommand::List => list_snapshots(&volume, &runtime),
            SnapshotCommand::Inspect(args) => inspect_snapshot(&volume, args.snapshot_id, &runtime),
            SnapshotCommand::Delete(args) => delete_snapshot(&volume, args.snapshot_id),
            SnapshotCommand::Policy(args) => match args.command {
                PolicyCommand::Set(args) => set_policy(&volume, args, &runtime),
                PolicyCommand::Show => show_policy(&volume, &runtime),
                PolicyCommand::Cancel => cancel_policy(&volume, &runtime),
            },
        }
    }
}

struct ResolvedVolume {
    format_info: FormatInfo,
    engine: Arc<PgSnapshotBackend>,
    context: SnapshotVolumeContext,
}

fn resolve_volume(client_root: &Path, volume_name: &str) -> Result<ResolvedVolume> {
    let path = crate::cli::utils::default_meta_dir_in_client_root(client_root, volume_name)
        .map_err(|error| anyhow!("{error}; usage: studiofs snapshot <VOLUME_NAME> <COMMAND>"))?
        .join(format!("{volume_name}.db"));
    if !path.is_file() {
        return Err(anyhow!(
            "no local metadata found for snapshot volume '{volume_name}'; initialize or discover this PostgreSQL-backed volume first; usage: studiofs snapshot <VOLUME_NAME> <COMMAND>"
        ));
    }
    let info = match mounted_format_info(volume_name)? {
        Some(info) => info,
        None => RedbMetaEngine::read_format_info_readonly(&path).map_err(|error| {
            snapshot_context_error(
                &path,
                format!("local metadata is corrupt or unsupported: {error}"),
            )
        })?,
    };
    if info.name != volume_name {
        return Err(snapshot_context_error(
            &path,
            "local volume identity does not match the requested volume name",
        ));
    }
    if info.metadata.backend != MetadataBackend::Postgres {
        return Err(snapshot_context_error(
            &path,
            "snapshots require a PostgreSQL-backed volume",
        ));
    }
    let postgres = info.metadata.postgres.as_ref().ok_or_else(|| {
        snapshot_context_error(&path, "PostgreSQL connection metadata is missing")
    })?;
    let hint =
        SnapshotVolumeIdentityHint::new(postgres.volume_id, info.name.clone(), info.uuid.clone())
            .map_err(|error| {
            snapshot_context_error(&path, format!("local volume identity is invalid: {error}"))
        })?;
    let engine = PgMetaEngine::open_snapshot_backend(&postgres.url, postgres.volume_id)
        .map_err(|error| snapshot_context_error(&path, error))?;
    let context = engine
        .validate_snapshot_volume_context(&hint)
        .map_err(|error| {
            snapshot_context_error(
                &path,
                format!("PostgreSQL volume identity validation failed: {error}"),
            )
        })?;
    Ok(ResolvedVolume {
        format_info: info,
        engine,
        context,
    })
}

fn mounted_format_info(volume_name: &str) -> Result<Option<FormatInfo>> {
    let active_mount = crate::cli::utils::active_mount_record_for_volume(volume_name)?;
    let Some(discovery) = crate::diagnostics::discover_local_sessions()?
        .into_iter()
        .find(|session| session.volume == volume_name)
    else {
        if active_mount.is_some() {
            return Err(anyhow!(
                "mounted volume metadata service is unavailable; stop the active mount or restart it with current diagnostics support"
            ));
        }
        return Ok(None);
    };
    let expected_uuid = discovery.volume_uuid.clone();
    let info = crate::runtime_bridge::block_on(async move {
        crate::diagnostics::LocalDiagnosticsClient::new(discovery)
            .snapshot_format_info()
            .await
    })
    .map_err(|error| anyhow!("mounted volume metadata is unavailable: {error}"))?;
    if info.name != volume_name || info.uuid != expected_uuid {
        return Err(anyhow!(
            "mounted volume metadata identity does not match the active mount"
        ));
    }
    Ok(Some(info))
}

fn runtime() -> Result<Runtime> {
    Runtime::new().context("create snapshot command runtime")
}

fn create_snapshot(
    volume: &ResolvedVolume,
    ttl: Option<Duration>,
    runtime: &Runtime,
) -> Result<()> {
    let retention = SnapshotRetention::manual(ttl).map_err(|error| anyhow!(error.to_string()))?;
    let entry = volume
        .engine
        .create_snapshot_v2(SnapshotOrigin::Manual, retention)
        .map_err(|error| anyhow!("PostgreSQL could not register the manual snapshot: {error}"))?;
    println!("Creating manual snapshot {}...", entry.snapshot_id);
    let worker = build_worker(volume)?;
    let cancel = CancellationToken::new();
    let signal_cancel = cancel.clone();
    let cancellation_requested = cancel.clone();
    let snapshot_id = entry.snapshot_id.clone();
    let ready = runtime.block_on(async {
        let signal_task = tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                signal_cancel.cancel();
            }
        });
        let result = worker.claim_or_follow(snapshot_id.clone(), cancel).await;
        signal_task.abort();
        let _ = signal_task.await;
        result
    });
    match ready {
        Ok(entry) => {
            println!("Snapshot {} is Ready.", entry.snapshot_id);
            print_catalog(&entry);
            Ok(())
        }
        Err(error) => {
            if cancellation_requested.is_cancelled() {
                return Err(anyhow!(
                    "snapshot creation interrupted. Registration '{}' remains active in PostgreSQL; track it with 'studiofs snapshot {:?} inspect {}'",
                    snapshot_id,
                    volume.context.volume_name,
                    snapshot_id
                ));
            }
            let failure = snapshot_jobs(volume)
                .ok()
                .and_then(|jobs| runtime.block_on(jobs.load_catalog(&snapshot_id)).ok())
                .and_then(|entry| entry.failure);
            Err(anyhow!(
                "snapshot {} did not become Ready: {}",
                snapshot_id,
                failure.unwrap_or_else(|| error.to_string())
            ))
        }
    }
}

fn list_snapshots(volume: &ResolvedVolume, runtime: &Runtime) -> Result<()> {
    let jobs = snapshot_jobs(volume)?;
    let entries = runtime.block_on(jobs.list_catalog());
    let entries = entries.map_err(|error| anyhow!("could not list snapshots: {error}"))?;
    if entries.is_empty() {
        println!("No snapshots for {}.", volume.context.volume_name);
    } else {
        for entry in entries {
            print_catalog(&entry);
        }
    }
    Ok(())
}

fn inspect_snapshot(
    volume: &ResolvedVolume,
    snapshot_id: SnapshotId,
    runtime: &Runtime,
) -> Result<()> {
    let entry = load_catalog(volume, &snapshot_id, runtime)?;
    print_catalog(&entry);
    Ok(())
}

fn delete_snapshot(volume: &ResolvedVolume, snapshot_id: SnapshotId) -> Result<()> {
    let entry = volume
        .engine
        .delete_snapshot_v2(&snapshot_id)
        .map_err(|error| anyhow!("could not request snapshot deletion: {error}"))?;
    println!(
        "Snapshot {} is {}.",
        entry.snapshot_id,
        lifecycle_name(entry.lifecycle)
    );
    println!("Existing mounts may continue until their leases drain.");
    Ok(())
}

fn set_policy(volume: &ResolvedVolume, args: PolicySetArgs, runtime: &Runtime) -> Result<()> {
    let timezone = args
        .timezone
        .unwrap_or_else(|| SnapshotDailyPolicy::DEFAULT_TIMEZONE.to_string());
    let policy = SnapshotDailyPolicy::new(args.daily_at, timezone, args.retention)
        .map_err(|error| anyhow!("invalid snapshot policy: {error}"))?;
    let store =
        volume.engine.clone() as Arc<dyn studiofs_meta::snapshot_ports::SnapshotPolicyStore>;
    let current = runtime
        .block_on(store.load_policy())
        .map_err(|error| anyhow!("could not load snapshot policy: {error}"))?;
    let expected_generation = current.as_ref().map(|record| record.generation);
    let record = runtime
        .block_on(store.set_policy(&policy, expected_generation))
        .map_err(|error| anyhow!("could not save snapshot policy: {error}"))?;
    println!(
        "Daily snapshots enabled at {} {} with {} retention (generation {}).",
        record.policy.daily_at(),
        record.policy.timezone(),
        format_retention(record.policy.retention()),
        record.generation
    );
    Ok(())
}

fn show_policy(volume: &ResolvedVolume, runtime: &Runtime) -> Result<()> {
    let store =
        volume.engine.clone() as Arc<dyn studiofs_meta::snapshot_ports::SnapshotPolicyStore>;
    match runtime
        .block_on(store.load_policy())
        .map_err(|error| anyhow!("could not load snapshot policy: {error}"))?
    {
        Some(record) => println!(
            "Daily snapshots: {} {}\nRetention: {}\nGeneration: {}",
            record.policy.daily_at(),
            record.policy.timezone(),
            format_retention(record.policy.retention()),
            record.generation
        ),
        None => println!("Daily snapshots: disabled"),
    }
    Ok(())
}

fn cancel_policy(volume: &ResolvedVolume, runtime: &Runtime) -> Result<()> {
    let store =
        volume.engine.clone() as Arc<dyn studiofs_meta::snapshot_ports::SnapshotPolicyStore>;
    let Some(record) = runtime
        .block_on(store.load_policy())
        .map_err(|error| anyhow!("could not load snapshot policy: {error}"))?
    else {
        println!("Daily snapshots are already disabled.");
        return Ok(());
    };
    runtime
        .block_on(store.cancel_policy(record.generation))
        .map_err(|error| anyhow!("could not cancel snapshot policy: {error}"))?;
    println!("Daily snapshot policy canceled. Existing snapshots are unaffected.");
    Ok(())
}

fn build_worker(volume: &ResolvedVolume) -> Result<Arc<SnapshotWorker>> {
    let jobs = snapshot_jobs(volume)?;
    let sources =
        volume.engine.clone() as Arc<dyn studiofs_meta::snapshot_ports::SnapshotSourceReader>;
    let data_store = Arc::new(
        DataStore::new(&DataStoreConfig::from(&volume.format_info))
            .map_err(|error| anyhow!("snapshot object storage is unavailable: {error}"))?,
    );
    let artifacts = Arc::new(
        DataStoreSnapshotArtifactStore::new(data_store.clone(), volume.context.clone()).map_err(
            |error| anyhow!("snapshot artifact storage configuration is invalid: {error}"),
        )?,
    );
    let payloads = Arc::new(
        DataStoreSnapshotPayloadReader::new(data_store, volume.context.clone()).map_err(
            |error| anyhow!("snapshot payload storage configuration is invalid: {error}"),
        )?,
    );
    let worker_id =
        studiofs_meta::snapshot_ports::SnapshotWorkerId::new(format!("cli-{}", std::process::id()))
            .map_err(|error| anyhow!("snapshot worker identity is invalid: {error}"))?;
    let client_root = crate::cli::utils::get_client_root()?;
    let materializer_root = crate::cli::utils::default_meta_dir_in_client_root(
        &client_root,
        &volume.context.volume_name,
    )?
    .join("cache")
    .join("snapshot-v2")
    .join("materializer");
    SnapshotWorker::new(
        volume.context.clone(),
        worker_id,
        jobs,
        sources,
        payloads,
        artifacts,
        Arc::new(SystemSnapshotClock),
        SnapshotWorkerConfig::new(materializer_root),
    )
    .map(Arc::new)
    .map_err(|error| anyhow!("snapshot worker configuration is invalid: {error}"))
}

fn snapshot_jobs(
    volume: &ResolvedVolume,
) -> Result<Arc<dyn studiofs_meta::snapshot_ports::SnapshotJobStore>> {
    Ok(volume.engine.clone() as Arc<dyn studiofs_meta::snapshot_ports::SnapshotJobStore>)
}

fn load_catalog(
    volume: &ResolvedVolume,
    snapshot_id: &SnapshotId,
    runtime: &Runtime,
) -> Result<SnapshotCatalogEntry> {
    let entry = runtime
        .block_on(snapshot_jobs(volume)?.load_catalog(snapshot_id))
        .map_err(|error| anyhow!("could not load snapshot: {error}"))?;
    if entry.volume != volume.context {
        return Err(anyhow!("snapshot belongs to a different volume"));
    }
    Ok(entry)
}

fn print_catalog(entry: &SnapshotCatalogEntry) {
    let checksum = entry
        .artifact
        .as_ref()
        .map(|artifact| artifact.sha256.to_hex())
        .unwrap_or_else(|| "-".to_string());
    println!(
        "{} origin={} captured_at={} cut_version={} state={} creation={} ready_at={} expires_at={} checksum={} last_error={}",
        entry.snapshot_id,
        origin_name(entry.origin),
        entry.cut.captured_at().to_rfc3339(),
        entry.cut.cut_version(),
        lifecycle_name(entry.lifecycle),
        entry.creation_state.as_str(),
        entry
            .ready_at
            .map_or_else(|| "-".to_string(), |value| value.to_rfc3339()),
        entry
            .expires_at
            .map_or_else(|| "-".to_string(), |value| value.to_rfc3339()),
        checksum,
        format_catalog_error(entry.failure.as_deref())
    );
}

fn format_catalog_error(error: Option<&str>) -> String {
    let Some(error) = error else {
        return "-".to_string();
    };
    if error.chars().all(|character| {
        !character.is_whitespace()
            && !character.is_control()
            && character != '"'
            && character != '\\'
    }) {
        error.to_string()
    } else {
        format!("{error:?}")
    }
}

fn origin_name(origin: SnapshotOrigin) -> &'static str {
    match origin {
        SnapshotOrigin::Manual => "manual",
        SnapshotOrigin::Automatic => "automatic",
    }
}

fn lifecycle_name(lifecycle: SnapshotLifecycle) -> &'static str {
    match lifecycle {
        SnapshotLifecycle::Creating => "Creating",
        SnapshotLifecycle::Ready => "Ready",
        SnapshotLifecycle::Failed => "Failed",
        SnapshotLifecycle::Deleting => "Deleting",
        SnapshotLifecycle::Deleted => "Deleted",
    }
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    if seconds == 0 {
        return "0s".to_string();
    }
    if seconds.is_multiple_of(7 * 24 * 60 * 60) {
        format!("{}w", seconds / (7 * 24 * 60 * 60))
    } else if seconds.is_multiple_of(24 * 60 * 60) {
        format!("{}d", seconds / (24 * 60 * 60))
    } else if seconds.is_multiple_of(60 * 60) {
        format!("{}h", seconds / (60 * 60))
    } else if seconds.is_multiple_of(60) {
        format!("{}m", seconds / 60)
    } else {
        format!("{seconds}s")
    }
}

fn format_retention(retention: SnapshotRetention) -> String {
    retention
        .ttl()
        .map(format_duration)
        .unwrap_or_else(|| "infinite".to_string())
}

fn parse_daily_at(value: &str) -> Result<NaiveTime, String> {
    if value.len() != 5
        || value.as_bytes()[2] != b':'
        || !value
            .bytes()
            .enumerate()
            .all(|(index, byte)| index == 2 || byte.is_ascii_digit())
    {
        return Err("expected a local time in HH:MM format".to_string());
    }
    NaiveTime::parse_from_str(value, "%H:%M")
        .map_err(|_| "expected a local time in HH:MM format".to_string())
}

fn parse_timezone(value: &str) -> Result<String, String> {
    SnapshotDailyPolicy::new(NaiveTime::MIN, value, Duration::from_secs(1))
        .map(|_| value.to_string())
        .map_err(|_| "expected a valid IANA timezone name".to_string())
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let split_at = value
        .find(|character: char| !character.is_ascii_digit())
        .ok_or_else(|| "expected a duration such as 30m, 7d, or 1w".to_string())?;
    let (number, suffix) = value.split_at(split_at);
    if number.is_empty() {
        return Err("expected a duration such as 30m, 7d, or 1w".to_string());
    }
    let amount = number
        .parse::<u64>()
        .map_err(|_| "duration value is out of range".to_string())?;
    let multiplier = match suffix {
        "s" => 1,
        "m" => 60,
        "h" => 60 * 60,
        "d" => 24 * 60 * 60,
        "w" => 7 * 24 * 60 * 60,
        _ => return Err("duration suffix must be s, m, h, d, or w".to_string()),
    };
    let seconds = amount
        .checked_mul(multiplier)
        .ok_or_else(|| "duration value is out of range".to_string())?;
    if seconds == 0 {
        return Err("duration must be greater than zero".to_string());
    }
    Ok(Duration::from_secs(seconds))
}

fn snapshot_context_error(path: &Path, error: impl std::fmt::Display) -> anyhow::Error {
    anyhow!(
        "snapshot volume context at '{}' is unavailable: {error}; initialize or rediscover the named PostgreSQL-backed volume; usage: studiofs snapshot <VOLUME_NAME> <COMMAND>",
        path.display()
    )
}

#[cfg(test)]
mod tests {
    use super::super::parse_for_test;
    use super::{SnapshotCommand, format_catalog_error, format_duration, resolve_volume};
    use std::fs;
    use std::time::Duration;

    #[test]
    fn invalid_volume_names_fail_before_reading_metadata() {
        let root = studiofs_test_support::workspace_target_tempdir("snapshot-named-volume-");
        for volume_name in ["", "..", "junk:", "junk.", "../outside", "slash/name"] {
            let error = resolve_volume(root.path(), volume_name)
                .err()
                .expect("reject invalid name");
            assert!(error.to_string().contains("invalid volume name"));
            assert!(
                error
                    .to_string()
                    .contains("studiofs snapshot <VOLUME_NAME> <COMMAND>")
            );
        }
        assert!(!root.path().join("meta").exists());
    }

    #[test]
    fn unknown_named_volume_does_not_read_other_volumes() {
        let root = studiofs_test_support::workspace_target_tempdir("snapshot-named-volume-");
        for volume_name in ["alpha", "beta"] {
            let dir = root.path().join("meta").join(volume_name);
            fs::create_dir_all(&dir).expect("create unrelated volume directory");
            fs::write(
                dir.join(format!("{volume_name}.db")),
                b"corrupt unrelated metadata",
            )
            .unwrap();
        }
        let error = resolve_volume(root.path(), "unknown")
            .err()
            .expect("missing named volume");
        assert!(
            error
                .to_string()
                .contains("no local metadata found for snapshot volume 'unknown'")
        );
        assert!(!error.to_string().contains("corrupt"));
        assert!(
            error
                .to_string()
                .contains("studiofs snapshot <VOLUME_NAME> <COMMAND>")
        );
    }

    #[test]
    fn corrupt_database_for_valid_volume_remains_an_error() {
        let root = studiofs_test_support::workspace_target_tempdir("snapshot-candidate-");
        let db_path = root.path().join("meta").join("valid").join("valid.db");
        fs::create_dir_all(db_path.parent().unwrap()).expect("create volume metadata directory");
        fs::write(&db_path, b"not a redb database").expect("write corrupt database");

        let error = match resolve_volume(root.path(), "valid") {
            Err(error) => error,
            Ok(_) => panic!("corrupt metadata for a valid candidate must fail closed"),
        };
        assert!(
            error.to_string().contains("corrupt or unsupported"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn snapshot_parser_accepts_the_canonical_forms() {
        for args in [
            &["snapshot", "production", "create"][..],
            &["snapshot", "production", "create", "--ttl", "7d"],
            &["snapshot", "production", "list"],
            &[
                "snapshot",
                "production",
                "inspect",
                "production.2026-08-11T20-58-31.482Z",
            ],
            &[
                "snapshot",
                "production",
                "delete",
                "production.2026-08-11T20-58-31.482Z",
            ],
            &[
                "snapshot",
                "production",
                "policy",
                "set",
                "--daily-at",
                "02:30",
                "--retention",
                "7d",
            ],
            &["snapshot", "production", "policy", "show"],
            &["snapshot", "production", "policy", "cancel"],
            &[
                "smount",
                "production",
                "production.2026-08-11T20-58-31.482Z",
                "Z:",
            ],
        ] {
            parse_for_test(args).unwrap_or_else(|error| panic!("{args:?}: {error}"));
        }
    }

    #[test]
    fn snapshot_parser_accepts_reserved_and_unicode_volume_names() {
        for volume_name in [
            "create",
            "list",
            "inspect",
            "delete",
            "policy",
            "help",
            "команда обсяг",
        ] {
            let super::super::Commands::Snapshot(parsed) =
                parse_for_test(&["snapshot", volume_name, "list"])
                    .expect("parse named snapshot command")
                    .command
            else {
                panic!("expected snapshot command");
            };
            assert_eq!(parsed.volume_name, volume_name);
            assert!(matches!(parsed.command, SnapshotCommand::List));
        }
    }

    #[test]
    fn snapshot_help_shows_the_required_volume_and_policy_commands() {
        let help = parse_for_test(&["snapshot", "--help"])
            .err()
            .expect("snapshot help should exit during parsing");
        assert_eq!(help.kind(), clap::error::ErrorKind::DisplayHelp);
        assert!(
            help.to_string()
                .contains("studiofs snapshot <VOLUME_NAME> [OPTIONS] <COMMAND>")
        );
        assert!(help.to_string().contains("policy"));

        let nested_help = parse_for_test(&["snapshot", "volume", "policy", "--help"])
            .err()
            .expect("policy help should exit during parsing");
        assert_eq!(nested_help.kind(), clap::error::ErrorKind::DisplayHelp);
        assert!(nested_help.to_string().contains("set"));
        assert!(nested_help.to_string().contains("show"));
    }

    #[test]
    fn snapshot_parser_rejects_forbidden_or_noncanonical_forms() {
        for args in [
            &["snapshot", "create"][..],
            &["snapshot", "create", "--ttl", "7d"],
            &["snapshot", "list"],
            &["snapshot", "inspect", "production.2026-08-11T20-58-31.482Z"],
            &["snapshot", "delete", "production.2026-08-11T20-58-31.482Z"],
            &["snapshot", "policy", "show"],
            &["snapshot", "policy", "cancel"],
            &[
                "snapshot",
                "policy",
                "set",
                "--daily-at",
                "02:30",
                "--retention",
                "7d",
            ],
            &["snapshot", "create", "production"][..],
            &["snapshot", "production", "create", "--root-inode", "1"],
            &[
                "snapshot",
                "production",
                "policy",
                "set",
                "--at",
                "02:30",
                "--retention",
                "7d",
            ],
            &[
                "snapshot",
                "production",
                "policy",
                "set",
                "--daily-at",
                "02:30",
                "--ttl",
                "7d",
            ],
            &["snapshot", "production", "policy", "restart"],
            &["smount", "production", "snapshot-id", "Z:", "--read-only"],
            &["smount", "production", "snapshot-id", "Z:", "--writable"],
        ] {
            assert!(
                parse_for_test(args).is_err(),
                "accepted forbidden form: {args:?}"
            );
        }
    }

    #[test]
    fn snapshot_parser_rejects_invalid_time_syntax_timezones_and_durations() {
        assert!(
            parse_for_test(&[
                "snapshot",
                "production",
                "policy",
                "set",
                "--daily-at",
                "2:30",
                "--retention",
                "7d"
            ])
            .is_err()
        );
        assert!(
            parse_for_test(&[
                "snapshot",
                "production",
                "policy",
                "set",
                "--daily-at",
                "02:30",
                "--retention",
                "0d"
            ])
            .is_err()
        );
        assert!(
            parse_for_test(&[
                "snapshot",
                "production",
                "policy",
                "set",
                "--daily-at",
                "02:30",
                "--timezone",
                "Not/AZone",
                "--retention",
                "7d"
            ])
            .is_err()
        );
        for value in ["d", "-5d"] {
            assert!(
                parse_for_test(&["snapshot", "production", "create", "--ttl", value,]).is_err(),
                "accepted malformed duration: {value}"
            );
        }
    }

    #[test]
    fn format_duration_zero_is_seconds() {
        assert_eq!(format_duration(Duration::ZERO), "0s");
    }

    #[test]
    fn format_duration_uses_the_largest_exact_unit() {
        assert_eq!(
            format_duration(Duration::from_secs(14 * 24 * 60 * 60)),
            "2w"
        );
        assert_eq!(format_duration(Duration::from_secs(25 * 60 * 60)), "25h");
        assert_eq!(format_duration(Duration::from_secs(90 * 60)), "90m");
        assert_eq!(format_duration(Duration::from_secs(45)), "45s");
    }

    #[test]
    fn format_catalog_error_quotes_unescaped_text() {
        assert_eq!(format_catalog_error(None), "-");
        assert_eq!(format_catalog_error(Some("timeout")), "timeout");
        assert_eq!(
            format_catalog_error(Some("connection reset by peer")),
            "\"connection reset by peer\""
        );
        assert_eq!(
            format_catalog_error(Some("quoted \"error\"")),
            "\"quoted \\\"error\\\"\""
        );
    }
}

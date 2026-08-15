//! `studiofs snapshot` — PostgreSQL-authoritative Snapshot v2 operations.

use anyhow::{Context, Result, anyhow};
use chrono::NaiveTime;
use clap::{Args, Subcommand};
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
use studiofs_meta::{
    FormatInfo, MetaEngine, MetadataBackend, PgMetaEngine, PgOpenOptions, RedbMetaEngine,
};
use studiofs_vfs::{
    DataStoreSnapshotArtifactStore, DataStoreSnapshotPayloadReader, SnapshotWorker,
    SnapshotWorkerConfig, SystemSnapshotClock,
};

#[derive(Debug, Args)]
#[command(about = "Create and recover whole-volume Snapshot v2 views")]
pub(crate) struct SnapshotArgs {
    #[command(subcommand)]
    command: SnapshotCommand,
}

#[derive(Debug, Subcommand)]
enum SnapshotCommand {
    /// Create a manual snapshot and wait until its artifact is Ready.
    Create(CreateArgs),
    /// List snapshots for the current live volume.
    List,
    /// Inspect one snapshot by its PostgreSQL-allocated identifier.
    Inspect(InspectArgs),
    /// Request idempotent deletion of one snapshot.
    Delete(DeleteArgs),
    /// Configure, inspect, or cancel the daily automatic snapshot policy.
    Policy(PolicyArgs),
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
    pub(crate) fn dispatch(self) -> Result<()> {
        let runtime = runtime()?;
        match self.command {
            SnapshotCommand::Create(args) => create_snapshot(args.ttl, &runtime),
            SnapshotCommand::List => list_snapshots(&runtime),
            SnapshotCommand::Inspect(args) => inspect_snapshot(args.snapshot_id, &runtime),
            SnapshotCommand::Delete(args) => delete_snapshot(args.snapshot_id),
            SnapshotCommand::Policy(args) => match args.command {
                PolicyCommand::Set(args) => set_policy(args, &runtime),
                PolicyCommand::Show => show_policy(&runtime),
                PolicyCommand::Cancel => cancel_policy(&runtime),
            },
        }
    }
}

struct ResolvedVolume {
    format_info: FormatInfo,
    engine: Arc<PgMetaEngine>,
    context: SnapshotVolumeContext,
}

fn resolve_volume() -> Result<ResolvedVolume> {
    let client_root = crate::cli::utils::get_client_root()?;
    let meta_root = client_root.join("meta");
    let mut candidates = std::fs::read_dir(&meta_root)
        .map_err(|_| anyhow!("no snapshot volume context found under '{}'; initialize a PostgreSQL-backed volume first", meta_root.display()))?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            entry
                .file_type()
                .ok()
                .filter(std::fs::FileType::is_dir)
                .and_then(|_| entry.file_name().into_string().ok())
        })
        .collect::<Vec<_>>();
    candidates.sort();
    if candidates.is_empty() {
        return Err(anyhow!(
            "no snapshot volume context found under '{}'; initialize a PostgreSQL-backed volume first",
            meta_root.display()
        ));
    }

    let mut resolved = Vec::new();
    let mut failures = Vec::new();
    for volume_name in candidates {
        match resolve_volume_candidate(&client_root, &volume_name) {
            Ok(Some(volume)) => resolved.push(volume),
            Ok(None) => {}
            Err(error) => failures.push(error),
        }
    }

    resolved.sort_by(|left, right| left.context.volume_name.cmp(&right.context.volume_name));
    resolved.dedup_by(|left, right| left.context == right.context);
    match resolved.len() {
        0 => match failures.into_iter().next() {
            Some(error) => Err(error),
            None => Err(anyhow!(
                "no PostgreSQL-backed snapshot volume context found under '{}'; initialize a PostgreSQL-backed volume first",
                meta_root.display()
            )),
        },
        1 => resolved
            .pop()
            .ok_or_else(|| anyhow!("resolved snapshot volume disappeared")),
        _ => Err(anyhow!(
            "multiple snapshot volume contexts found: {}; run the command with one client root containing exactly one current PostgreSQL-backed volume",
            resolved
                .iter()
                .map(|volume| volume.context.volume_name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

fn resolve_volume_candidate(
    client_root: &Path,
    volume_name: &str,
) -> Result<Option<ResolvedVolume>> {
    let path = crate::cli::utils::default_meta_dir_in_client_root(client_root, volume_name)
        .join(format!("{volume_name}.db"));
    if !path.is_file() {
        return Ok(None);
    }
    let info = RedbMetaEngine::read_format_info_readonly(&path).map_err(|error| {
        snapshot_context_error(
            &path,
            format!("local metadata is corrupt or unsupported: {error}"),
        )
    })?;
    if info.metadata.backend != MetadataBackend::Postgres {
        return Ok(None);
    }
    let postgres = info.metadata.postgres.as_ref().ok_or_else(|| {
        snapshot_context_error(&path, "PostgreSQL connection metadata is missing")
    })?;
    let hint =
        SnapshotVolumeIdentityHint::new(postgres.volume_id, info.name.clone(), info.uuid.clone())
            .map_err(|error| {
            snapshot_context_error(&path, format!("local volume identity is invalid: {error}"))
        })?;
    let engine = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .map_err(|error| snapshot_context_error(&path, error))?;
    let context = engine
        .validate_snapshot_volume_context(&hint)
        .map_err(|error| {
            snapshot_context_error(
                &path,
                format!("PostgreSQL volume identity validation failed: {error}"),
            )
        })?;
    Ok(Some(ResolvedVolume {
        format_info: info,
        engine,
        context,
    }))
}

fn runtime() -> Result<Runtime> {
    Runtime::new().context("create snapshot command runtime")
}

fn create_snapshot(ttl: Option<Duration>, runtime: &Runtime) -> Result<()> {
    let volume = resolve_volume()?;
    let retention = SnapshotRetention::manual(ttl).map_err(|error| anyhow!(error.to_string()))?;
    let entry = volume
        .engine
        .create_snapshot_v2(SnapshotOrigin::Manual, retention)
        .map_err(|error| anyhow!("PostgreSQL could not register the manual snapshot: {error}"))?;
    println!("Creating manual snapshot {}...", entry.snapshot_id);
    let worker = build_worker(&volume)?;
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
                println!(
                    "Snapshot creation interrupted. Registration '{}' remains active in PostgreSQL; track it with 'studiofs snapshot inspect {}'.",
                    snapshot_id, snapshot_id
                );
                return Ok(());
            }
            let failure = snapshot_jobs(&volume)
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

fn list_snapshots(runtime: &Runtime) -> Result<()> {
    let volume = resolve_volume()?;
    let jobs = snapshot_jobs(&volume)?;
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

fn inspect_snapshot(snapshot_id: SnapshotId, runtime: &Runtime) -> Result<()> {
    let volume = resolve_volume()?;
    let entry = load_catalog(&volume, &snapshot_id, runtime)?;
    print_catalog(&entry);
    Ok(())
}

fn delete_snapshot(snapshot_id: SnapshotId) -> Result<()> {
    let volume = resolve_volume()?;
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

fn set_policy(args: PolicySetArgs, runtime: &Runtime) -> Result<()> {
    let volume = resolve_volume()?;
    let timezone = args
        .timezone
        .unwrap_or_else(|| SnapshotDailyPolicy::DEFAULT_TIMEZONE.to_string());
    let policy = SnapshotDailyPolicy::new(args.daily_at, timezone, args.retention)
        .map_err(|error| anyhow!("invalid snapshot policy: {error}"))?;
    let store = volume
        .engine
        .snapshot_policy_store()
        .ok_or_else(|| anyhow!("daily snapshot policy requires a PostgreSQL-backed volume"))?;
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

fn show_policy(runtime: &Runtime) -> Result<()> {
    let volume = resolve_volume()?;
    let store = volume
        .engine
        .snapshot_policy_store()
        .ok_or_else(|| anyhow!("daily snapshot policy requires a PostgreSQL-backed volume"))?;
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

fn cancel_policy(runtime: &Runtime) -> Result<()> {
    let volume = resolve_volume()?;
    let store = volume
        .engine
        .snapshot_policy_store()
        .ok_or_else(|| anyhow!("daily snapshot policy requires a PostgreSQL-backed volume"))?;
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
    let sources = volume
        .engine
        .snapshot_source_reader()
        .ok_or_else(|| anyhow!("snapshot source reader is unavailable"))?;
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
    )
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
    volume
        .engine
        .snapshot_job_store()
        .ok_or_else(|| anyhow!("snapshot jobs require a PostgreSQL-backed volume"))
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
        entry.cut.captured_at(),
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
        entry.failure.as_deref().unwrap_or("-")
    );
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
        "snapshot volume context at '{}' is unavailable: {error}; initialize or rediscover the current PostgreSQL-backed volume",
        path.display()
    )
}

#[cfg(test)]
mod tests {
    use super::super::parse_for_test;
    use super::format_duration;
    use std::time::Duration;

    #[test]
    fn snapshot_parser_accepts_the_canonical_forms() {
        for args in [
            &["snapshot", "create"][..],
            &["snapshot", "create", "--ttl", "7d"],
            &["snapshot", "list"],
            &["snapshot", "inspect", "production.2026-08-11T20-58-31.482Z"],
            &["snapshot", "delete", "production.2026-08-11T20-58-31.482Z"],
            &[
                "snapshot",
                "policy",
                "set",
                "--daily-at",
                "02:30",
                "--retention",
                "7d",
            ],
            &["snapshot", "policy", "show"],
            &["snapshot", "policy", "cancel"],
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
    fn snapshot_parser_rejects_forbidden_or_noncanonical_forms() {
        for args in [
            &["snapshot", "create", "production"][..],
            &["snapshot", "create", "--root-inode", "1"],
            &[
                "snapshot",
                "policy",
                "set",
                "--at",
                "02:30",
                "--retention",
                "7d",
            ],
            &[
                "snapshot",
                "policy",
                "set",
                "--daily-at",
                "02:30",
                "--ttl",
                "7d",
            ],
            &["snapshot", "policy", "restart"],
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
                parse_for_test(&["snapshot", "create", "--ttl", value,]).is_err(),
                "accepted malformed duration: {value}"
            );
        }
    }

    #[test]
    fn format_duration_zero_is_seconds() {
        assert_eq!(format_duration(Duration::ZERO), "0s");
    }
}

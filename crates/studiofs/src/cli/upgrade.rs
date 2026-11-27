//! Explicit, retryable schema migration and format upgrade for a stopped local
//! volume cache.

use anyhow::{Context, bail};
use clap::{Args, ValueEnum};
use std::collections::BTreeSet;
use std::time::Duration;
use studiofs_meta::RedbMetaEngine;
use studiofs_meta::compat::ClientCapabilities;
use studiofs_meta::pg_engine::migrations::{migrate_registered_schema, registered_schema_status};
use studiofs_meta::pg_engine::{execute_feature_cutover, inspect_cutover_dry_run};
use studiofs_meta::schema::{FormatInfo, PostgresConfig};
use studiofs_meta::volume_protocol::{
    CapabilityRole, CutoverDryRunReport, CutoverPhase, CutoverRequest, OfflineClientPolicy,
};

const BACKUP_GUIDANCE: &str = "Before upgrading: stop every mount and janitor of every volume in this PostgreSQL database, back up the database (for example with pg_dump), and run `studiofs dump` with the version that last opened each volume. Schema migrations are database-wide and never move back.";

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum CliCutoverPhase {
    Expand,
    Migrate,
    Activate,
    Deactivate,
    Contract,
}

impl From<CliCutoverPhase> for CutoverPhase {
    fn from(value: CliCutoverPhase) -> Self {
        match value {
            CliCutoverPhase::Expand => Self::Expand,
            CliCutoverPhase::Migrate => Self::Migrate,
            CliCutoverPhase::Activate => Self::Activate,
            CliCutoverPhase::Deactivate => Self::Deactivate,
            CliCutoverPhase::Contract => Self::Contract,
        }
    }
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum CliCapabilityRole {
    Read,
    Write,
    Maintain,
}

impl From<CliCapabilityRole> for CapabilityRole {
    fn from(value: CliCapabilityRole) -> Self {
        match value {
            CliCapabilityRole::Read => Self::Read,
            CliCapabilityRole::Write => Self::Write,
            CliCapabilityRole::Maintain => Self::Maintain,
        }
    }
}

#[derive(Args, Debug)]
pub struct UpgradeArgs {
    /// Locally discovered volume; stop all its mounts and janitors first (except for `--feature` cutover inspection).
    pub name: String,
    /// Report the schema migrations, format upgrade, or feature cutover readiness without changing anything.
    #[arg(long)]
    pub dry_run: bool,
    /// Re-apply the 0.2.5 baseline schema to restore dropped objects; allowed only while no later schema migration is recorded.
    #[arg(long)]
    pub repair_schema: bool,
    /// Cache directory used by mount, when set with its --cache-dir option.
    #[arg(long)]
    pub cache_dir: Option<std::path::PathBuf>,
    /// Feature capability name for an explicit Expand-Migrate-Activate-Contract cutover.
    #[arg(long, requires = "phase", conflicts_with = "repair_schema")]
    pub feature: Option<String>,
    /// Cutover phase (`expand`, `migrate`, `activate`, `deactivate`, `contract`).
    #[arg(
        long,
        value_enum,
        requires = "feature",
        conflicts_with = "repair_schema"
    )]
    pub phase: Option<CliCutoverPhase>,
    /// Capability role(s) (`read`, `write`, `maintain`) when activating a feature.
    #[arg(long = "role", value_enum, requires = "feature")]
    pub roles: Vec<CliCapabilityRole>,
    /// Explicit write_requirement override for feature activation or deactivation.
    #[arg(long, requires = "feature")]
    pub write_requirement: Option<u16>,
    /// Explicitly accept fencing offline clients that may hold unknown unpublished local work.
    #[arg(long, requires = "feature")]
    pub accept_offline_clients: bool,
    /// Expected policy epoch for optimistic concurrency when applying a feature cutover.
    #[arg(long, requires = "feature")]
    pub expected_epoch: Option<u64>,
    /// Upgrade only the stopped local metadata database.
    #[cfg(feature = "test-successor")]
    #[arg(long, conflicts_with_all = ["repair_schema", "dry_run", "feature"])]
    pub local_only: bool,
}

impl UpgradeArgs {
    pub fn dispatch(self) -> anyhow::Result<()> {
        let path =
            super::utils::get_default_meta_dir(&self.name)?.join(format!("{}.db", self.name));
        if !path.is_file() {
            bail!(
                "Local volume metadata is missing; run upgrade where this volume already has a stopped cache."
            );
        }
        if let (Some(feature), Some(phase)) = (self.feature.clone(), self.phase) {
            return self.dispatch_feature_cutover(&path, feature, phase.into());
        }
        if super::utils::active_mount_record_for_volume(&self.name)?.is_some() {
            bail!("Unmount this volume before running upgrade.");
        }
        #[cfg(feature = "test-successor")]
        if self.local_only {
            let local = RedbMetaEngine::open_for_format_upgrade(&path).context(
                "Cannot own local metadata; stop every process using this volume and retry",
            )?;
            let format = local.format_info_for_upgrade()?;
            if format.name != self.name {
                bail!("Local volume name does not match its header; metadata was not changed.");
            }
            local.upgrade_local_revision()?;
            return Ok(());
        }
        let runtime = tokio::runtime::Runtime::new()?;
        if self.dry_run {
            // Read-only: the exclusive open below creates missing tables and commits.
            let format = RedbMetaEngine::read_format_info_for_upgrade_readonly(&path)
                .context("Cannot read local metadata; nothing was changed")?;
            let pg = postgres_binding(&format, &self.name)?;
            let report = runtime
                .block_on(async {
                    tokio::time::timeout(
                        Duration::from_secs(45),
                        dry_run_report(&format, pg, self.repair_schema),
                    )
                    .await
                })
                .context("Schema preflight timed out; nothing was changed")?
                .context("Schema preflight failed; nothing was changed")?;
            println!("{report}{BACKUP_GUIDANCE}");
            return Ok(());
        }
        // redb owns the same exclusive database lock required by mount startup.
        // Keep it through shared and local completion, including retries.
        let local = RedbMetaEngine::open_for_format_upgrade(&path)
            .context("Cannot own local metadata; stop every process using this volume and retry")?;
        let format = local.format_info_for_upgrade()?;
        let pg = postgres_binding(&format, &self.name)?;
        let settings = super::settings::load_studiofs_config()?;
        let cache_dir =
            super::mount::resolve_cache_dir_for_mount(&settings, self.cache_dir.as_deref())
                .unwrap_or_else(|| path.with_file_name("cache"));
        let layout = studiofs_cache::runtime::LocalPayloadLayout::from_cache_root(
            cache_dir.join(&format.uuid),
        );
        let uuid = &format.uuid;
        let repair_schema = self.repair_schema;
        let shared = runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(45), super::init::with_postgres_client(&pg.url, |mut client| async move {
                for step in migrate_registered_schema(&mut client, repair_schema).await? {
                    println!("Applied schema migration {step}.");
                }
                if format.format_version == 5 {
                    studiofs_meta::pg_engine::upgrade_shared_format5(&mut client, pg.volume_id, uuid).await?;
                }
                Ok(studiofs_meta::pg_engine::upgrade_shared_format6(&mut client, pg.volume_id, uuid).await?)
            })).await.context("Upgrade timed out; retry the same command to complete safely")?
        }).context("Shared upgrade did not complete locally. Committed schema migrations stay applied and a failed one is rolled back. Stop all volume mounts and janitors, verify database connectivity and schema privileges, then retry this command. Accepted local data is preserved.")?;
        finish_local_upgrade(&local, &format, &shared, &layout.redb_index_path)
            .context("Shared format is upgraded; local completion failed. Keep this cache and retry upgrade; do not reinitialize or clear it")?;
        println!(
            "Volume '{}' now uses format {}. Repeat upgrade on every other stopped cache before remounting.",
            self.name, shared.format_version
        );
        Ok(())
    }

    fn dispatch_feature_cutover(
        self,
        path: &std::path::Path,
        feature: String,
        phase: CutoverPhase,
    ) -> anyhow::Result<()> {
        let (format, local_state) = RedbMetaEngine::read_upgrade_and_cutover_state_readonly(path)
            .context(
                "Cannot read local metadata; stop the mount or close any process holding the local cache and retry. Nothing was changed",
            )?;
        let pg = postgres_binding(&format, &self.name)?;
        let roles: BTreeSet<CapabilityRole> = self.roles.into_iter().map(Into::into).collect();
        let request = CutoverRequest {
            feature,
            phase,
            roles,
            write_requirement: self.write_requirement,
            offline_client_policy: if self.accept_offline_clients {
                OfflineClientPolicy::AcceptFence
            } else {
                OfflineClientPolicy::BlockUnknownWork
            },
        };
        let actor = ClientCapabilities::current();
        let runtime = tokio::runtime::Runtime::new()?;
        if self.dry_run {
            let report = runtime
                .block_on(async {
                    tokio::time::timeout(
                        Duration::from_secs(45),
                        super::init::with_postgres_client(&pg.url, |client| async move {
                            Ok(inspect_cutover_dry_run(
                                &client,
                                pg.volume_id,
                                &request,
                                &actor,
                                &local_state,
                            )
                            .await?)
                        }),
                    )
                    .await
                })
                .context("Feature cutover dry-run timed out; nothing was changed")?
                .context("Feature cutover dry-run failed; nothing was changed")?;
            print!("{}", format_cutover_report(&report, true));
            return Ok(());
        }
        let expected_epoch = self.expected_epoch;
        let report = runtime
            .block_on(async {
                tokio::time::timeout(
                    Duration::from_secs(45),
                    super::init::with_postgres_client(&pg.url, |mut client| async move {
                        Ok(execute_feature_cutover(
                            &mut client,
                            pg.volume_id,
                            expected_epoch,
                            &request,
                            &actor,
                            "studiofs upgrade",
                            &local_state,
                        )
                        .await?)
                    }),
                )
                .await
            })
            .context("Feature cutover timed out")?
            .context("Feature cutover was not applied")?;
        print!("{}", format_cutover_report(&report, false));
        Ok(())
    }
}

fn format_cutover_report(report: &CutoverDryRunReport, dry_run: bool) -> String {
    let mut out = String::new();
    if dry_run {
        out.push_str("Dry run: nothing was changed.\n");
    } else {
        out.push_str("Feature cutover applied.\n");
    }
    out.push_str(&format!(
        "Feature: {} (phase: {})\nEpoch: {} -> {}\nRollback boundary: {}\n",
        report.feature,
        report.phase.as_str(),
        report.current_epoch,
        report.next_epoch,
        report.rollback_boundary
    ));
    out.push_str(&format!(
        "Sessions: {} eligible active, {} blocking active, {} expired unsupported, {} stale-epoch active\n",
        report.sessions.eligible_active.len(),
        report.sessions.blocking_active.len(),
        report.sessions.expired_unsupported.len(),
        report.sessions.stale_epoch_active.len()
    ));
    if report.blockers.is_empty() {
        out.push_str("Blockers: none\n");
    } else {
        out.push_str("Blockers:\n");
        for blocker in &report.blockers {
            out.push_str(&format!("  - {blocker}\n"));
        }
    }
    out
}

fn finish_local_upgrade(
    local: &RedbMetaEngine,
    previous: &FormatInfo,
    shared: &FormatInfo,
    index_path: &std::path::Path,
) -> anyhow::Result<()> {
    if index_path.try_exists()? {
        studiofs_cache::runtime::CacheIndex::upgrade_existing(index_path)?;
    } else {
        // Opening the cache later migrates the index; say so instead of implying it ran.
        println!(
            "No payload index at {}; if mount uses another --cache-dir, it upgrades there on first open.",
            index_path.display()
        );
    }
    if previous.format_version == 5 {
        let mut intermediate = shared.clone();
        intermediate.format_version = 6;
        local.upgrade_local_format5(&intermediate)?;
    }
    local.upgrade_local_format6(shared)?;
    Ok(())
}

fn postgres_binding<'a>(format: &'a FormatInfo, name: &str) -> anyhow::Result<&'a PostgresConfig> {
    if format.name != name {
        bail!("Local volume name does not match its header; metadata was not changed.");
    }
    format
        .metadata
        .postgres
        .as_ref()
        .context("This volume has no PostgreSQL authority binding")
}

async fn dry_run_report(
    format: &FormatInfo,
    pg: &PostgresConfig,
    repair_schema: bool,
) -> anyhow::Result<String> {
    super::init::with_postgres_client(&pg.url, |client| async move {
        let (recorded, pending) = registered_schema_status(&client, repair_schema).await?;
        // Backend error text may echo connection data; keep only the operation.
        let shared: Option<i16> = client
            .query_opt(
                "SELECT metadata_format_version FROM metadata_state WHERE vol=$1",
                &[&pg.volume_id],
            )
            .await
            .map_err(|_| anyhow::anyhow!("reading the shared volume format failed"))?
            .map(|row| row.get(0));
        let mut report = String::from("Dry run: nothing was changed.\nSchema components:\n");
        for row in &recorded {
            report.push_str(&format!("  {} = {}\n", row.component, row.version));
        }
        if recorded.is_empty() {
            report.push_str("  none recorded\n");
        }
        report.push_str("Schema migrations to apply:\n");
        for step in &pending {
            report.push_str(&format!("  {step}\n"));
        }
        if pending.is_empty() {
            report.push_str("  none\n");
        }
        report.push_str(&format!(
            "Volume format: local {}, shared {}; upgrade writes {}.\n",
            format.format_version,
            shared.map_or_else(|| "missing".to_owned(), |v| v.to_string()),
            studiofs_meta::schema::CURRENT_FORMAT_VERSION
        ));
        Ok(report)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::UpgradeArgs;

    fn parse(args: &[&str]) -> Option<UpgradeArgs> {
        match super::super::parse_for_test(args).ok()?.command {
            super::super::Commands::Upgrade(args) => Some(args),
            _ => None,
        }
    }

    #[test]
    fn upgrade_requires_exactly_one_volume() {
        assert!(super::super::parse_for_test(&["upgrade", "volume"]).is_ok());
        assert!(super::super::parse_for_test(&["upgrade"]).is_err());
        assert!(super::super::parse_for_test(&["upgrade", "one", "two"]).is_err());
    }

    #[test]
    fn dry_run_is_an_opt_in_flag() {
        assert!(!parse(&["upgrade", "volume"]).unwrap().dry_run);
        assert!(parse(&["upgrade", "volume", "--dry-run"]).unwrap().dry_run);
        assert!(!parse(&["upgrade", "volume"]).unwrap().repair_schema);
        assert!(
            parse(&["upgrade", "volume", "--repair-schema"])
                .unwrap()
                .repair_schema
        );
    }

    #[cfg(feature = "test-successor")]
    #[test]
    fn local_only_flag_conflicts_with_remote_flags() {
        assert!(!parse(&["upgrade", "volume"]).unwrap().local_only);
        assert!(
            parse(&["upgrade", "volume", "--local-only"])
                .unwrap()
                .local_only
        );
        assert!(
            super::super::parse_for_test(&["upgrade", "volume", "--local-only", "--dry-run"])
                .is_err()
        );
        assert!(
            super::super::parse_for_test(&["upgrade", "volume", "--local-only", "--repair-schema"])
                .is_err()
        );
    }

    #[test]
    fn feature_cutover_flags_parse_and_require_each_other() {
        assert!(
            super::super::parse_for_test(&["upgrade", "volume", "--feature", "metadata.v1"])
                .is_err()
        );
        assert!(
            super::super::parse_for_test(&["upgrade", "volume", "--phase", "activate"]).is_err()
        );
        let parsed = parse(&[
            "upgrade",
            "volume",
            "--feature",
            "metadata.v1",
            "--phase",
            "activate",
            "--role",
            "read",
            "--role",
            "write",
            "--write-requirement",
            "2",
            "--accept-offline-clients",
            "--expected-epoch",
            "3",
            "--dry-run",
        ])
        .unwrap();
        assert_eq!(parsed.feature.as_deref(), Some("metadata.v1"));
        assert_eq!(parsed.phase, Some(super::CliCutoverPhase::Activate));
        assert_eq!(
            parsed.roles,
            vec![
                super::CliCapabilityRole::Read,
                super::CliCapabilityRole::Write
            ]
        );
        assert_eq!(parsed.write_requirement, Some(2));
        assert!(parsed.accept_offline_clients);
        assert_eq!(parsed.expected_epoch, Some(3));
        assert!(parsed.dry_run);
    }

    #[test]
    fn cutover_reads_local_redb_unpublished_work_and_lag() {
        use std::collections::BTreeSet;
        use studiofs_meta::compat::ClientCapabilities;
        use studiofs_meta::engine::{MetaFormat, MetaFsOps, MetaInodes};
        use studiofs_meta::schema::{
            FormatInfo, FormatInfoDataStore, MetadataBackend, PostgresConfig,
        };
        use studiofs_meta::volume_protocol::{
            CapabilityRole, CutoverBlocker, CutoverPhase, CutoverRequest, CutoverSharedState,
            FeatureState, OfflineClientPolicy, VolumeProtocol, evaluate_cutover_readiness,
        };
        use studiofs_meta::{ROOT_INO, RedbMetaEngine};

        let dir = studiofs_test_support::workspace_target_tempdir("upgrade-cutover-local-");
        let db_path = dir.path().join("cutover.db");
        let mut info = FormatInfo::new(
            "cutover-test".to_owned(),
            uuid::Uuid::new_v4().to_string(),
            FormatInfoDataStore {
                endpoint: "http://127.0.0.1:9000".to_owned(),
                bucket: "bucket".to_owned(),
                access_key: "ak".to_owned(),
                secret_key: "sk".to_owned(),
                region: "us-east-1".to_owned(),
            },
            1,
        )
        .expect("format info");
        info.metadata.postgres = Some(PostgresConfig {
            url: "postgres://user:pass@127.0.0.1:5432/db".to_owned(),
            volume_id: 1,
        });
        info.metadata.backend = MetadataBackend::Postgres;

        let engine = RedbMetaEngine::open(&db_path).expect("open redb");
        engine.set_format_info(&info).expect("set format info");
        engine.init_root(0, 0).expect("init root");
        // Append one local unpublished event and leave local version behind remote head.
        engine
            .create_dir(ROOT_INO, b"pending-dir", 0, 0, 0o755)
            .expect("create local dir");
        let next_event_seq = engine
            .get_counter(studiofs_meta::schema::counters::NEXT_EVENT_SEQ)
            .expect("next event seq");
        drop(engine);

        let (_read_info, dirty_local) =
            RedbMetaEngine::read_upgrade_and_cutover_state_readonly(&db_path)
                .expect("read cutover local state");
        assert!(dirty_local.unpublished_events > 0);
        assert_eq!(dirty_local.local_version, Some(0));

        let feature = "metadata.cli_cutover.v1";
        let mut actor = ClientCapabilities::current();
        actor.read.insert(feature);
        let mut current = VolumeProtocol::baseline();
        current
            .requirements
            .features
            .insert(feature.to_owned(), FeatureState::Compatible);
        let req = CutoverRequest {
            feature: feature.to_owned(),
            phase: CutoverPhase::Activate,
            roles: BTreeSet::from([CapabilityRole::Read]),
            write_requirement: None,
            offline_client_policy: OfflineClientPolicy::BlockUnknownWork,
        };
        let shared = CutoverSharedState {
            remote_head_version: 3,
            checkpoint_version: 3,
            checkpoint_ready: true,
            available_checkpoints: vec![current.requirements.checkpoint],
            snapshot_or_gc_active: false,
            active_maintenance_detail: None,
            database_volume_blockers: Vec::new(),
        };

        let dirty_report =
            evaluate_cutover_readiness(&current, &req, &actor, &[], &dirty_local, &shared);
        assert!(!dirty_report.is_ready());
        assert!(dirty_report.blockers.iter().any(|b| matches!(
            b,
            CutoverBlocker::DirtyUnpublishedLocalWork {
                unpublished_events
            } if *unpublished_events > 0
        )));
        assert!(dirty_report.blockers.iter().any(|b| matches!(
            b,
            CutoverBlocker::ReplicationLag {
                local_version: 0,
                remote_head_version: 3
            }
        )));

        // Mark all local events flushed and local version caught up to remote head.
        let engine = RedbMetaEngine::open(&db_path).expect("reopen redb");
        engine
            .set_counter(
                studiofs_meta::schema::counters::LOCAL_PG_FLUSHED_EVENT_SEQ,
                next_event_seq,
            )
            .expect("ack local event");
        engine
            .set_counter("pgLastAppliedBatchVersion", 3)
            .expect("catch up local version");
        drop(engine);

        let (_read_info, clean_local) =
            RedbMetaEngine::read_upgrade_and_cutover_state_readonly(&db_path)
                .expect("read clean cutover local state");
        assert_eq!(clean_local.unpublished_events, 0);
        assert_eq!(clean_local.local_version, Some(3));

        let clean_report =
            evaluate_cutover_readiness(&current, &req, &actor, &[], &clean_local, &shared);
        assert!(clean_report.is_ready(), "{:?}", clean_report.blockers);
    }
}

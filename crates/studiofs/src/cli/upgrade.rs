//! Explicit, retryable schema migration and format upgrade for a stopped local
//! volume cache.

use anyhow::{Context, bail};
use clap::Args;
use std::time::Duration;
use studiofs_meta::pg_engine::migrations::{migrate_registered_schema, registered_schema_status};
use studiofs_meta::schema::{FormatInfo, PostgresConfig};
use studiofs_meta::{RedbMetaEngine, decode_format_info};

const BACKUP_GUIDANCE: &str = "Before upgrading: stop every mount and janitor of every volume in this PostgreSQL database, back up the database (for example with pg_dump), and run `studiofs dump` for each volume. Schema migrations are database-wide and never move back.";

#[derive(Args, Debug)]
pub struct UpgradeArgs {
    /// Locally discovered volume; stop all its mounts and janitors first.
    pub name: String,
    /// Report the schema migrations and format upgrade that would run, without changing anything.
    #[arg(long)]
    pub dry_run: bool,
    /// Re-apply the 0.2.5 baseline schema to restore dropped objects; allowed only while no later schema migration is recorded.
    #[arg(long)]
    pub repair_schema: bool,
    /// Upgrade only the stopped local metadata database.
    #[cfg(feature = "test-successor")]
    #[arg(long, conflicts_with_all = ["repair_schema", "dry_run"])]
    pub local_only: bool,
}

impl UpgradeArgs {
    pub fn dispatch(self) -> anyhow::Result<()> {
        if super::utils::active_mount_record_for_volume(&self.name)?.is_some() {
            bail!("Unmount this volume before running upgrade.");
        }
        let path =
            super::utils::get_default_meta_dir(&self.name)?.join(format!("{}.db", self.name));
        if !path.is_file() {
            bail!("Local volume metadata is missing; discover the volume before upgrading.");
        }
        #[cfg(feature = "test-successor")]
        if self.local_only {
            let local = RedbMetaEngine::open_for_format_upgrade(&path)?;
            return Ok(local.upgrade_local_revision()?);
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
        // Keep it through both commits, including recovery after a lost response.
        let local = RedbMetaEngine::open_for_format_upgrade(&path)
            .context("Cannot own local metadata; stop every process using this volume and retry")?;
        let format = local.format_info_for_upgrade()?;
        let pg = postgres_binding(&format, &self.name)?;
        let uuid = &format.uuid;
        let repair_schema = self.repair_schema;
        let shared = runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(45), super::init::with_postgres_client(&pg.url, |mut client| async move {
                for step in migrate_registered_schema(&mut client, repair_schema).await? {
                    println!("Applied schema migration {step}.");
                }
                studiofs_meta::pg_engine::upgrade_shared_format5(&mut client, pg.volume_id, uuid).await?;
                let row = client.query_one("SELECT format_info FROM metadata_state WHERE vol=$1", &[&pg.volume_id]).await?;
                let bytes: Vec<u8> = row.get(0);
                Ok(decode_format_info(&bytes)?)
            })).await.context("Upgrade timed out; retry the same command to complete safely")?
        }).context("Shared upgrade did not complete locally. Committed schema migrations stay applied and a failed one is rolled back. Stop all volume mounts and janitors, verify database connectivity and schema privileges, then retry this command. Accepted local data is preserved.")?;
        local.upgrade_local_format5(&shared)
            .context("Shared format is upgraded; local completion failed. Keep this cache and retry upgrade; do not reinitialize or clear it")?;
        println!(
            "Volume '{}' uses format 6. Repeat upgrade on every other stopped cache before remounting. Use an updated janitor: payload_gc schema 2 fences older janitors across this database.",
            self.name
        );
        Ok(())
    }
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
}

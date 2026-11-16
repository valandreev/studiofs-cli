//! Explicit, retryable format upgrade for a stopped local volume cache.

use anyhow::{Context, bail};
use clap::Args;
use std::time::Duration;
use studiofs_meta::{RedbMetaEngine, decode_format_info};

#[derive(Args, Debug)]
pub struct UpgradeArgs {
    /// Locally discovered volume; stop all its mounts and janitors first.
    pub name: String,
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
        // redb owns the same exclusive database lock required by mount startup.
        // Keep it through both commits, including recovery after a lost response.
        let local = RedbMetaEngine::open_for_format_upgrade(&path)
            .context("Cannot own local metadata; stop every process using this volume and retry")?;
        let format = local.format_info_for_upgrade()?;
        if format.name != self.name {
            bail!("Local volume name does not match its header; metadata was not changed.");
        }
        let pg = format
            .metadata
            .postgres
            .as_ref()
            .context("This volume has no PostgreSQL authority binding")?;
        let runtime = tokio::runtime::Runtime::new()?;
        let shared = runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(45), super::init::with_postgres_client(&pg.url, |mut client| async move {
                let tx = client.transaction().await?;
                tx.batch_execute("SET LOCAL lock_timeout = '2s'").await?;
                tx.execute("SELECT pg_advisory_xact_lock($1)", &[&super::init::INIT_SCHEMA_ADVISORY_LOCK]).await?;
                studiofs_meta::pg_engine::schema::ensure_studiofs_pg_schema(&tx).await?;
                tx.commit().await?;
                studiofs_meta::pg_engine::upgrade_shared_format5(&mut client, pg.volume_id, &format.uuid).await?;
                let row = client.query_one("SELECT format_info FROM metadata_state WHERE vol=$1", &[&pg.volume_id]).await?;
                let bytes: Vec<u8> = row.get(0);
                Ok(decode_format_info(&bytes)?)
            })).await.context("Upgrade timed out; retry the same command to complete safely")?
        }).context("Shared upgrade did not complete locally. Stop all volume mounts and janitors, verify database connectivity and schema privileges, then retry this command. Accepted local data is preserved.")?;
        local.upgrade_local_format5(&shared)
            .context("Shared format is upgraded; local completion failed. Keep this cache and retry upgrade; do not reinitialize or clear it")?;
        println!(
            "Volume '{}' uses format 6. Repeat upgrade on every other stopped cache before remounting. Use an updated janitor: payload_gc schema 2 fences older janitors across this database.",
            self.name
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn upgrade_requires_exactly_one_volume() {
        assert!(super::super::parse_for_test(&["upgrade", "volume"]).is_ok());
        assert!(super::super::parse_for_test(&["upgrade"]).is_err());
        assert!(super::super::parse_for_test(&["upgrade", "one", "two"]).is_err());
    }
}

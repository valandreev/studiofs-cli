//! Shared logical byte quota configuration and status.

use anyhow::{Context, bail};
use clap::Args;
use std::time::Duration;
use studiofs_meta::trash::VolumeSettings;

#[derive(Args, Debug)]
pub struct QuotaArgs {
    /// Locally discovered volume; it may be mounted.
    pub name: String,
    /// Maximum logical file bytes, or `unlimited` to remove the limit. Omit to show status.
    #[arg(value_parser = validate_limit)]
    pub limit: Option<String>,
}

impl QuotaArgs {
    pub fn dispatch(self) -> anyhow::Result<()> {
        let limit = self.limit.as_deref().map(parse_limit).transpose()?;
        let format = super::trash::local_format_info(&self.name)?;
        let pg = format
            .metadata
            .postgres
            .as_ref()
            .context("This volume has no PostgreSQL authority binding")?;
        let volume = pg.volume_id;
        let uuid = format.uuid;
        let name = self.name;
        let runtime = tokio::runtime::Runtime::new()?;
        let settings = runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(30), super::init::with_postgres_client(&pg.url, |mut client| async move {
                let row = client.query_one("SELECT uuid, volume_settings FROM volumes v JOIN metadata_state ms ON ms.vol=v.vol WHERE v.vol=$1", &[&volume]).await?;
                if row.get::<_, String>(0) != uuid { bail!("shared volume identity mismatch"); }
                let bytes: Option<Vec<u8>> = row.get(1);
                let current = bytes.map(|b| serde_json::from_slice::<VolumeSettings>(&b)).transpose()?.unwrap_or_default();
                let Some(limit) = limit else { return Ok(current); };
                let client_id = format!("cli-quota-{}", uuid::Uuid::new_v4());
                let now_ms = i64::try_from(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis())?;
                studiofs_meta::pg_engine::set_shared_logical_quota_bytes(&mut client, volume, Some(current.revision), limit, &client_id, now_ms).await?.context("volume settings changed concurrently; retry the command")
            })).await
        }).context("PostgreSQL did not answer in time; nothing is confirmed, retry the command")?
          .context("Could not reach the shared volume settings; check PostgreSQL connectivity and retry")?;
        match settings.logical_quota_bytes {
            Some(bytes) => println!("Volume '{name}': logical quota is {bytes} bytes."),
            None => println!("Volume '{name}': logical quota is unlimited."),
        }
        println!(
            "Settings revision {}; mounted clients apply it after replication. Usage and admission are local-first, so clients may temporarily overshoot the shared limit.",
            settings.revision
        );
        Ok(())
    }
}

fn parse_limit(value: &str) -> anyhow::Result<Option<u64>> {
    if value == "unlimited" {
        return Ok(None);
    }
    value
        .parse::<u64>()
        .map(Some)
        .context("quota must be a nonnegative whole number of bytes or `unlimited`")
}

fn validate_limit(value: &str) -> Result<String, String> {
    parse_limit(value)
        .map(|_| value.to_owned())
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::super::parse_for_test;

    #[test]
    fn quota_cli_accepts_bytes_unlimited_and_status() {
        assert!(parse_for_test(&["quota", "volume"]).is_ok());
        assert!(parse_for_test(&["quota", "volume", "0"]).is_ok());
        assert!(parse_for_test(&["quota", "volume", "18446744073709551615"]).is_ok());
        assert!(parse_for_test(&["quota", "volume", "unlimited"]).is_ok());
        for invalid in ["-1", "1.5", "18446744073709551616", "1GiB", ""] {
            assert!(
                parse_for_test(&["quota", "volume", invalid]).is_err(),
                "{invalid:?} must be rejected"
            );
        }
        assert!(parse_for_test(&["quota"]).is_err());
    }
}

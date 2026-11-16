//! Shared per-volume trash retention setting (whole days, 0 disables).

use anyhow::{Context, bail};
use clap::Args;
use std::time::Duration;
use studiofs_meta::RedbMetaEngine;
use studiofs_meta::trash::VolumeSettings;

#[derive(Args, Debug)]
pub struct TrashDaysArgs {
    /// Locally discovered volume; it may be mounted.
    pub name: String,
    /// Whole days to keep native Move to Trash entries; 0 stops new retention.
    /// Omit to show the current shared setting.
    pub days: Option<u32>,
}

impl TrashDaysArgs {
    pub fn dispatch(self) -> anyhow::Result<()> {
        let path =
            super::utils::get_default_meta_dir(&self.name)?.join(format!("{}.db", self.name));
        if !path.is_file() {
            bail!("Local volume metadata is missing; discover the volume first.");
        }
        let format = match super::snapshot::mounted_format_info(&self.name)? {
            Some(format) => format,
            None => RedbMetaEngine::read_format_info_readonly(&path)
                .context("Local volume metadata is unreadable or needs `studiofs upgrade`")?,
        };
        if format.name != self.name {
            bail!("Local volume name does not match its header; nothing was changed.");
        }
        let pg = format
            .metadata
            .postgres
            .as_ref()
            .context("This volume has no PostgreSQL authority binding")?;
        let (volume, uuid, days) = (pg.volume_id, format.uuid.clone(), self.days);
        let runtime = tokio::runtime::Runtime::new()?;
        let settings = runtime
            .block_on(async { tokio::time::timeout(
                Duration::from_secs(30),
                super::init::with_postgres_client(&pg.url, |mut client| async move {
                    let row = client
                        .query_one("SELECT uuid, volume_settings FROM volumes v JOIN metadata_state ms ON ms.vol=v.vol WHERE v.vol=$1", &[&volume])
                        .await?;
                    if row.get::<_, String>(0) != uuid {
                        bail!("shared volume identity mismatch");
                    }
                    let Some(days) = days else {
                        let bytes: Option<Vec<u8>> = row.get(1);
                        return Ok(bytes
                            .map(|bytes| serde_json::from_slice::<VolumeSettings>(&bytes))
                            .transpose()?
                            .unwrap_or_default());
                    };
                    let now_ms = i64::try_from(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)?
                            .as_millis(),
                    )?;
                    let client_id = format!("cli-trash-days-{}", uuid::Uuid::new_v4());
                    studiofs_meta::pg_engine::set_shared_trash_days(
                        &mut client, volume, None, days, &client_id, now_ms,
                    )
                    .await?
                    .context("unconditional settings update returned no result")
                })
            ).await })
            .context("PostgreSQL did not answer in time; nothing is confirmed, retry the command")?
            .map_err(|_| anyhow::anyhow!("Could not reach the shared volume settings. Check PostgreSQL connectivity and that every cache uses format 6, then retry."))?;
        print_settings(&self.name, settings);
        Ok(())
    }
}

fn print_settings(name: &str, settings: VolumeSettings) {
    let policy = settings.trash;
    if policy.enabled() {
        println!(
            "Volume '{name}': native Move to Trash keeps entries for {} day(s).",
            policy.days()
        );
    } else if policy.existing_entry_days() > 0 {
        println!(
            "Volume '{name}': new trash retention is disabled; existing entries still expire after {} day(s).",
            policy.existing_entry_days()
        );
    } else {
        println!("Volume '{name}': trash is disabled (0 days).");
    }
    println!(
        "Settings revision {}; mounted clients apply it after replication.",
        settings.revision
    );
}

#[cfg(test)]
mod tests {
    use super::super::parse_for_test;

    #[test]
    fn trash_days_accepts_only_whole_nonnegative_days() {
        assert!(parse_for_test(&["trash-days", "volume"]).is_ok());
        assert!(parse_for_test(&["trash-days", "volume", "0"]).is_ok());
        assert!(parse_for_test(&["trash-days", "volume", "30"]).is_ok());
        assert!(parse_for_test(&["trash-days", "volume", "4294967295"]).is_ok());
        for invalid in ["-1", "1.5", "4294967296", "7d", ""] {
            assert!(
                parse_for_test(&["trash-days", "volume", invalid]).is_err(),
                "{invalid:?} must be rejected"
            );
        }
        assert!(parse_for_test(&["trash-days"]).is_err());
        let init = |days: &str| {
            parse_for_test(&[
                "init",
                "v",
                "--endpoint",
                "http://127.0.0.1:9",
                "--bucket",
                "b",
                "--access-key",
                "a",
                "--secret-key",
                "s",
                "--meta",
                "postgres://x",
                "--trash-days",
                days,
            ])
        };
        assert!(init("14").is_ok());
        assert!(init("-1").is_err() && init("0.5").is_err());
    }
}

//! Shared per-volume trash retention setting (whole days, 0 disables).

use anyhow::{Context, bail};
use clap::Args;
use studiofs_meta::trash::VolumeSettings;
use studiofs_meta::{FormatInfo, RedbMetaEngine};

pub(super) fn local_format_info(name: &str) -> anyhow::Result<FormatInfo> {
    let path = super::utils::get_default_meta_dir(name)?.join(format!("{name}.db"));
    if !path.is_file() {
        bail!("Local volume metadata is missing; discover the volume first.");
    }
    let format = match super::snapshot::mounted_format_info(name)? {
        Some(format) => format,
        None => RedbMetaEngine::read_format_info_readonly(&path)
            .context("Local volume metadata is unreadable or needs `studiofs upgrade`")?,
    };
    if format.name != name {
        bail!("Local volume name does not match its header; nothing was changed.");
    }
    Ok(format)
}

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
        let settings = super::volume_settings::read_or_update_shared_settings(
            &self.name,
            self.days
                .map(super::volume_settings::SettingsUpdate::TrashDays),
            "Could not reach the shared volume settings. Check PostgreSQL connectivity and that every cache uses the current volume format, then retry.",
        )?;
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

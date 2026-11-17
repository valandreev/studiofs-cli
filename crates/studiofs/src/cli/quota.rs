//! Shared logical byte quota configuration and status.

use anyhow::Context;
use clap::Args;

#[derive(Args, Debug)]
pub struct QuotaArgs {
    /// Locally discovered volume; it may be mounted.
    pub name: String,
    /// Maximum logical size (e.g. 100M, 100MB, 1.5G), or unlimited. Units use powers of 1024. Omit to show status.
    #[arg(value_parser = validate_limit)]
    pub limit: Option<String>,
}

impl QuotaArgs {
    pub fn dispatch(self) -> anyhow::Result<()> {
        let limit = self.limit.as_deref().map(parse_limit).transpose()?;
        let name = self.name;
        let settings = super::volume_settings::read_or_update_shared_settings(
            &name,
            limit.map(super::volume_settings::SettingsUpdate::LogicalQuota),
            "Could not reach the shared volume settings; check PostgreSQL connectivity and retry",
        )?;
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
    studiofs_vfs::parse_size_bytes(value)
        .map(Some)
        .context("expected a size such as 100M, 100GB, a byte count, or unlimited")
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
        for (short, long, bytes) in [
            ("100M", "100MB", 100 << 20),
            ("100G", "100GB", 100 << 30),
            ("1.5k", "1.5KB", 1536),
        ] {
            for size in [short, long] {
                assert!(parse_for_test(&["quota", "volume", size]).is_ok());
                assert_eq!(super::parse_limit(size).unwrap(), Some(bytes));
            }
        }
        for invalid in ["-1", "M", ".", "1.5.0G", "18446744073709551616", "1GiB", ""] {
            assert!(
                parse_for_test(&["quota", "volume", invalid]).is_err(),
                "{invalid:?} must be rejected"
            );
        }
        assert!(parse_for_test(&["quota"]).is_err());
    }
}

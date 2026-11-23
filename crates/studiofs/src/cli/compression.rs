//! Shared remote payload compression policy configuration and status.

use anyhow::bail;
use clap::Args;
use studiofs_meta::trash::CompressionPolicy;

#[derive(Args, Debug)]
pub struct CompressionArgs {
    /// Locally discovered volume; it may be mounted.
    pub name: String,
    /// Compression policy (`off`, `zstd`, or `zstd:<level>` with level 1..=19). Omit to show status.
    #[arg(value_parser = validate_policy)]
    pub policy: Option<String>,
}

impl CompressionArgs {
    pub fn dispatch(self) -> anyhow::Result<()> {
        let policy = self.policy.as_deref().map(parse_policy).transpose()?;
        let name = self.name;
        let settings = super::volume_settings::read_or_update_shared_settings(
            &name,
            policy.map(super::volume_settings::SettingsUpdate::Compression),
            "Could not reach the shared volume settings; check PostgreSQL connectivity and retry",
        )?;
        println!("Volume '{name}': compression is {}.", settings.compression);
        println!(
            "Settings revision {}; mounted clients apply it after replication.",
            settings.revision
        );
        Ok(())
    }
}

fn parse_policy(value: &str) -> anyhow::Result<CompressionPolicy> {
    if value == "off" {
        return Ok(CompressionPolicy::off());
    }
    if value == "zstd" {
        return Ok(CompressionPolicy::zstd_default());
    }
    if let Some(level_str) = value.strip_prefix("zstd:") {
        if level_str.is_empty() || !level_str.bytes().all(|byte| byte.is_ascii_digit()) {
            bail!("expected 'off', 'zstd', or 'zstd:<level>' with level 1..=19");
        }
        let level: u8 = level_str
            .parse()
            .map_err(|_| anyhow::anyhow!("compression level must be between 1 and 19"))?;
        return CompressionPolicy::zstd(level).map_err(|error| anyhow::anyhow!(error));
    }
    bail!("expected 'off', 'zstd', or 'zstd:<level>' with level 1..=19")
}

fn validate_policy(value: &str) -> Result<String, String> {
    parse_policy(value)
        .map(|_| value.to_owned())
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::super::parse_for_test;
    use studiofs_meta::trash::{CompressionMode, CompressionPolicy};

    #[test]
    fn compression_cli_accepts_show_off_zstd_and_levels() {
        assert!(parse_for_test(&["compression", "volume"]).is_ok());
        assert!(parse_for_test(&["compression", "volume", "off"]).is_ok());
        assert!(parse_for_test(&["compression", "volume", "zstd"]).is_ok());
        assert!(parse_for_test(&["compression", "volume", "zstd:1"]).is_ok());
        assert!(parse_for_test(&["compression", "volume", "zstd:9"]).is_ok());
        assert!(parse_for_test(&["compression", "volume", "zstd:19"]).is_ok());

        assert_eq!(
            super::parse_policy("off").unwrap(),
            CompressionPolicy::off()
        );
        assert_eq!(
            super::parse_policy("zstd").unwrap(),
            CompressionPolicy::zstd_default()
        );
        let level9 = super::parse_policy("zstd:9").unwrap();
        assert_eq!(level9.mode(), CompressionMode::Zstd);
        assert_eq!(level9.level(), 9);

        for invalid in [
            "", "lz4", "off:3", "zstd:", "zstd:0", "zstd:20", "zstd:256", "zstd:-1", "zstd:+3",
            "zstd:abc", "ZSTD", "OFF",
        ] {
            assert!(
                parse_for_test(&["compression", "volume", invalid]).is_err(),
                "{invalid:?} must be rejected"
            );
        }
        assert!(parse_for_test(&["compression"]).is_err());
    }
}

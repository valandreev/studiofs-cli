//! Update mutable S3 settings stored in shared PostgreSQL volume metadata.

use anyhow::{Context, bail};
use clap::{ArgGroup, Args};
use std::fmt;
use std::time::Duration;
use studiofs_data::normalize_s3_endpoint_transport;
use studiofs_meta::connect_studiofs_postgres;
use studiofs_meta::pg_engine::S3VolumeSettingsUpdate;

#[derive(Args)]
#[command(
    group(ArgGroup::new("authority").args(["meta", "disk"]).required(true)),
    group(ArgGroup::new("updates").args(["endpoint", "bucket", "access_key", "secret_key", "region", "trash_days"]).required(true).multiple(true))
)]
pub struct UmetaArgs {
    /// PostgreSQL connection string for the shared metadata database
    #[arg(long, group = "authority")]
    meta: Option<String>,

    /// Use PostgreSQL connection details from an existing local disk
    #[arg(long, group = "authority")]
    disk: Option<String>,

    /// Disk name; required with --meta and optional with --disk
    #[arg(required_unless_present = "disk")]
    name: Option<String>,

    #[arg(long)]
    endpoint: Option<String>,
    #[arg(long)]
    bucket: Option<String>,
    #[arg(long)]
    access_key: Option<String>,
    #[arg(long)]
    secret_key: Option<String>,
    #[arg(long)]
    region: Option<String>,
    #[arg(long)]
    trash_days: Option<u32>,
}

impl fmt::Debug for UmetaArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UmetaArgs")
            .field(
                "authority",
                &if self.disk.is_some() { "disk" } else { "meta" },
            )
            .field("name", &self.name.as_deref().or(self.disk.as_deref()))
            .field("updates", &self.updated_fields())
            .finish()
    }
}

impl UmetaArgs {
    fn updated_fields(&self) -> Vec<&'static str> {
        let mut fields = Vec::new();
        if self.endpoint.is_some() {
            fields.push("endpoint");
        }
        if self.bucket.is_some() {
            fields.push("bucket");
        }
        if self.access_key.is_some() {
            fields.push("access-key");
        }
        if self.secret_key.is_some() {
            fields.push("secret-key");
        }
        if self.region.is_some() {
            fields.push("region");
        }
        if self.trash_days.is_some() {
            fields.push("trash-days");
        }
        fields
    }

    pub fn dispatch(self) -> anyhow::Result<()> {
        let name = match (&self.meta, &self.disk, &self.name) {
            (Some(_), None, Some(name)) => name.clone(),
            (None, Some(disk), optional_name) => {
                if optional_name.as_ref().is_some_and(|name| name != disk) {
                    bail!("positional disk name must match --disk");
                }
                disk.clone()
            }
            _ => bail!("provide either --meta DSN <disk-name> or --disk <disk-name>"),
        };
        if self.updated_fields().is_empty() {
            bail!("provide at least one setting to update");
        }

        let local_authority = if self.disk.is_some() {
            let format = super::trash::local_format_info(&name)?;
            let pg = format
                .metadata
                .postgres
                .as_ref()
                .context("This volume has no PostgreSQL authority binding")?;
            Some((pg.url.clone(), pg.volume_id, format.uuid))
        } else {
            None
        };
        let dsn = local_authority
            .as_ref()
            .map(|(dsn, _, _)| dsn.clone())
            .or_else(|| self.meta.clone())
            .context("missing PostgreSQL connection source")?;
        let endpoint = self
            .endpoint
            .as_deref()
            .map(normalize_s3_endpoint_transport)
            .transpose()
            .context("invalid S3 endpoint")?;
        for (field, value) in [
            ("bucket", self.bucket.as_deref()),
            ("access key", self.access_key.as_deref()),
            ("secret key", self.secret_key.as_deref()),
            ("endpoint", endpoint.as_deref()),
        ] {
            if value.is_some_and(|value| value.trim().is_empty()) {
                bail!("{field} must not be empty");
            }
        }
        let region = self.region.as_deref().map(|region| {
            if region.trim().is_empty() {
                "us-east-1"
            } else {
                region
            }
        });
        let update = S3VolumeSettingsUpdate {
            endpoint: endpoint.as_deref(),
            bucket: self.bucket.as_deref(),
            access_key: self.access_key.as_deref(),
            secret_key: self.secret_key.as_deref(),
            region,
            trash_days: self.trash_days,
        };
        let fields = self.updated_fields();
        let client_id = format!("cli-umeta-{}", uuid::Uuid::new_v4());
        let now_ms = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis(),
        )?;
        let runtime = tokio::runtime::Runtime::new()?;
        let lookup_name = name.clone();
        let use_disk_dsn = self.disk.is_some();
        runtime.block_on(async move {
            let dsn = if use_disk_dsn {
                dsn
            } else {
                studiofs_meta::resolve_studiofs_pg_dsns(&dsn)
                    .map_err(|_| anyhow::anyhow!("could not resolve StudioFS PostgreSQL database"))?
                    .metadata_dsn
            };
            let (mut client, connection) = connect_studiofs_postgres(&dsn).await.map_err(|_| {
                anyhow::anyhow!("could not connect to StudioFS PostgreSQL metadata")
            })?;
            let connection_task = tokio::spawn(async move {
                let _ = connection.await;
            });
            let timed_result = tokio::time::timeout(Duration::from_secs(30), async move {
                let (volume, uuid) = if let Some((_, volume, uuid)) = local_authority {
                    (volume, uuid)
                } else {
                    let rows = client
                        .query(
                            "SELECT v.vol, v.uuid, ms.format_info FROM volumes v JOIN metadata_state ms ON ms.vol=v.vol WHERE v.name=$1 LIMIT 2",
                            &[&lookup_name],
                        )
                        .await
                        .map_err(|_| anyhow::anyhow!("could not find shared volume metadata"))?;
                    let row = match rows.as_slice() {
                        [] => bail!("volume was not found in shared PostgreSQL metadata"),
                        [row] => row,
                        _ => bail!("volume name is ambiguous in shared PostgreSQL metadata"),
                    };
                    let volume: i64 = row.get(0);
                    let uuid: String = row.get(1);
                    let info = studiofs_meta::decode_format_info(&row.get::<_, Vec<u8>>(2))
                        .map_err(|_| anyhow::anyhow!("shared volume format is invalid"))?;
                    if info.uuid != uuid || info.name != lookup_name {
                        bail!("shared volume identity mismatch");
                    }
                    (volume, uuid)
                };
                studiofs_meta::pg_engine::update_shared_s3_settings(
                    &mut client,
                    volume,
                    &uuid,
                    update,
                    &client_id,
                    now_ms,
                )
                .await
                .map_err(|_| anyhow::anyhow!("could not update shared volume settings"))?;
                Ok::<_, anyhow::Error>(())
            })
            .await;
            connection_task.abort();
            let _ = connection_task.await;
            timed_result
                .map_err(|_| anyhow::anyhow!("PostgreSQL did not answer in time; nothing is confirmed, retry the command"))?
        })?;
        println!(
            "Updated shared settings for volume '{name}': {}.",
            fields.join(", ")
        );
        println!(
            "Upgraded running mounts pick up access/secret key changes after their next successful metadata poll; endpoint, bucket, and region changes need a remount."
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::parse_for_test;

    #[test]
    fn umeta_accepts_meta_and_disk_forms_and_hides_secrets_in_debug() {
        let meta = parse_for_test(&[
            "umeta",
            "--meta",
            "postgres://user:pass@db",
            "volume",
            "--access-key",
            "AKIA-secret",
            "--secret-key",
            "s3-secret",
        ])
        .unwrap();
        let crate::cli::Commands::Umeta(meta) = meta.command else {
            panic!("expected umeta")
        };
        let debug = format!("{meta:?}");
        assert!(
            !debug.contains("pass")
                && !debug.contains("AKIA-secret")
                && !debug.contains("s3-secret")
        );

        let disk = parse_for_test(&["umeta", "--disk", "volume", "--bucket", "new-bucket"])
            .expect("--disk form should parse");
        let crate::cli::Commands::Umeta(disk) = disk.command else {
            panic!("expected umeta")
        };
        assert!(format!("{disk:?}").contains("bucket"));
    }

    #[test]
    fn umeta_requires_exactly_one_authority_and_meta_name() {
        assert!(parse_for_test(&["umeta", "volume", "--bucket", "b"]).is_err());
        assert!(parse_for_test(&["umeta", "--meta", "postgres://db", "--bucket", "b"]).is_err());
        assert!(
            parse_for_test(&[
                "umeta",
                "--meta",
                "postgres://db",
                "--disk",
                "volume",
                "--bucket",
                "b"
            ])
            .is_err()
        );
        assert!(parse_for_test(&["umeta", "--disk", "volume", "other", "--bucket", "b"]).is_ok());
    }

    #[test]
    fn umeta_requires_an_update_during_argument_parsing() {
        assert!(parse_for_test(&["umeta", "--meta", "postgres://db", "volume"]).is_err());
    }

    #[test]
    fn umeta_rejects_a_positional_name_that_disagrees_with_disk() {
        let parsed = parse_for_test(&[
            "umeta",
            "--disk",
            "volume",
            "other",
            "--bucket",
            "new-bucket",
        ])
        .unwrap();
        let crate::cli::Commands::Umeta(args) = parsed.command else {
            panic!("expected umeta")
        };
        assert_eq!(
            args.dispatch().unwrap_err().to_string(),
            "positional disk name must match --disk"
        );
    }

    #[test]
    fn umeta_rejects_empty_s3_values_before_connecting() {
        for args in [
            ["umeta", "--meta", "postgres://db", "volume", "--bucket", ""],
            [
                "umeta",
                "--meta",
                "postgres://db",
                "volume",
                "--access-key",
                "",
            ],
            [
                "umeta",
                "--meta",
                "postgres://db",
                "volume",
                "--secret-key",
                "",
            ],
            [
                "umeta",
                "--meta",
                "postgres://db",
                "volume",
                "--access-key",
                "   ",
            ],
        ] {
            let parsed = parse_for_test(&args).expect("arguments should parse");
            let crate::cli::Commands::Umeta(args) = parsed.command else {
                panic!("expected umeta")
            };
            assert!(
                args.dispatch()
                    .unwrap_err()
                    .to_string()
                    .contains("must not be empty")
            );
        }
    }
}

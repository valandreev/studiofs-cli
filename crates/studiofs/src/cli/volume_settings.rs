//! Shared PostgreSQL read/update path for the mutable volume settings commands.

use anyhow::{Context, bail};
use std::time::Duration;
use studiofs_meta::trash::{CompressionPolicy, VolumeSettings};

const SETTINGS_TIMEOUT: Duration = Duration::from_secs(30);

/// One shared settings change, applied after the volume identity check.
pub(super) enum SettingsUpdate {
    /// Unconditionally replace the trash retention days.
    TrashDays(u32),
    /// Compare-and-set the logical quota against the revision just read.
    LogicalQuota(Option<u64>),
    /// Compare-and-set the compression policy against the revision just read.
    Compression(CompressionPolicy),
}

impl SettingsUpdate {
    fn client_id_prefix(&self) -> &'static str {
        match self {
            Self::TrashDays(_) => "cli-trash-days",
            Self::LogicalQuota(_) => "cli-quota",
            Self::Compression(_) => "cli-compression",
        }
    }

    fn missing_result_context(&self) -> &'static str {
        match self {
            Self::TrashDays(_) => "unconditional settings update returned no result",
            Self::LogicalQuota(_) | Self::Compression(_) => {
                "volume settings changed concurrently; retry the command"
            }
        }
    }

    async fn apply(
        self,
        client: &mut tokio_postgres::Client,
        volume: i64,
        current: &VolumeSettings,
        client_id: &str,
        now_ms: i64,
    ) -> anyhow::Result<Option<VolumeSettings>> {
        Ok(match self {
            Self::TrashDays(days) => {
                studiofs_meta::pg_engine::set_shared_trash_days(
                    client, volume, None, days, client_id, now_ms,
                )
                .await?
            }
            Self::LogicalQuota(bytes) => {
                studiofs_meta::pg_engine::set_shared_logical_quota_bytes(
                    client,
                    volume,
                    Some(current.revision),
                    bytes,
                    client_id,
                    now_ms,
                )
                .await?
            }
            Self::Compression(policy) => {
                studiofs_meta::pg_engine::set_shared_compression(
                    client,
                    volume,
                    Some(current.revision),
                    policy,
                    client_id,
                    now_ms,
                )
                .await?
            }
        })
    }
}

/// Reads the shared settings of a locally discovered volume, applying
/// `update` first when one is given. `unreachable_context` describes a
/// connection or query failure to the operator.
pub(super) fn read_or_update_shared_settings(
    name: &str,
    update: Option<SettingsUpdate>,
    unreachable_context: &'static str,
) -> anyhow::Result<VolumeSettings> {
    let format = super::trash::local_format_info(name)?;
    let pg = format
        .metadata
        .postgres
        .as_ref()
        .context("This volume has no PostgreSQL authority binding")?;
    let (volume, uuid) = (pg.volume_id, format.uuid.clone());
    let runtime = tokio::runtime::Runtime::new()?;
    runtime
        .block_on(async {
            tokio::time::timeout(
                SETTINGS_TIMEOUT,
                super::init::with_postgres_client(&pg.url, |mut client| async move {
                    let current = load_shared_settings(&client, volume, &uuid).await?;
                    let Some(update) = update else {
                        return Ok(current);
                    };
                    let client_id =
                        format!("{}-{}", update.client_id_prefix(), uuid::Uuid::new_v4());
                    let now_ms = i64::try_from(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)?
                            .as_millis(),
                    )?;
                    let missing = update.missing_result_context();
                    update
                        .apply(&mut client, volume, &current, &client_id, now_ms)
                        .await?
                        .context(missing)
                }),
            )
            .await
        })
        .context("PostgreSQL did not answer in time; nothing is confirmed, retry the command")?
        .context(unreachable_context)
}

async fn load_shared_settings(
    client: &tokio_postgres::Client,
    volume: i64,
    uuid: &str,
) -> anyhow::Result<VolumeSettings> {
    let row = client
        .query_one(
            "SELECT uuid, volume_settings FROM volumes v JOIN metadata_state ms ON ms.vol=v.vol WHERE v.vol=$1",
            &[&volume],
        )
        .await?;
    if row.get::<_, String>(0) != uuid {
        bail!("shared volume identity mismatch");
    }
    let bytes: Option<Vec<u8>> = row.get(1);
    Ok(bytes
        .map(|bytes| serde_json::from_slice::<VolumeSettings>(&bytes))
        .transpose()?
        .unwrap_or_default())
}

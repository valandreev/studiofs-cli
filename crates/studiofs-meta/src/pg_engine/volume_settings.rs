use super::*;
use crate::MetaEvent;
use crate::trash::{MetaVolumeSettings, VolumeSettings};

#[derive(Default)]
/// Optional mutable S3 and trash values applied in one shared metadata transaction.
pub struct S3VolumeSettingsUpdate<'a> {
    /// Replacement S3 endpoint.
    pub endpoint: Option<&'a str>,
    /// Replacement object-storage bucket.
    pub bucket: Option<&'a str>,
    /// Replacement S3 access key.
    pub access_key: Option<&'a str>,
    /// Replacement S3 secret key.
    pub secret_key: Option<&'a str>,
    /// Replacement S3 region.
    pub region: Option<&'a str>,
    /// Replacement trash retention in whole days.
    pub trash_days: Option<u32>,
}

impl crate::trash::MetaRetainedStore for PgMetaEngine {
    fn protect_retained_file_payload(
        &self,
        receipt: Ino,
        source: crate::trash::RetainedSourceFence,
        stage: &[u8],
    ) -> MetaResult<InodeAttr> {
        self.mutate(|local| local.protect_retained_file_payload(receipt, source, stage))
    }
    fn retained_entry(&self, receipt: Ino) -> MetaResult<Option<crate::trash::RetainedEntry>> {
        self.local.retained_entry(receipt)
    }
    fn retain_namespace_entry(
        &self,
        parent: Ino,
        name: &[u8],
        store: Ino,
        receipt: Ino,
    ) -> MetaResult<crate::trash::RetainedEntry> {
        self.mutate(|local| local.retain_namespace_entry(parent, name, store, receipt))
    }
    fn restore_retained_entry(
        &self,
        receipt: Ino,
        parent: Ino,
        name: &[u8],
        stage: &[u8],
        sources: &[crate::trash::RetainedSourceFence],
    ) -> MetaResult<crate::trash::RetainedRestoreResult> {
        self.mutate(|local| local.restore_retained_entry(receipt, parent, name, stage, sources))
    }
    fn request_retained_purge(
        &self,
        receipt: Ino,
        reason: crate::trash::RetainedPurgeReason,
    ) -> MetaResult<crate::trash::RetainedEntry> {
        self.mutate(|local| local.request_retained_purge(receipt, reason))
    }
    fn purge_retained_child(
        &self,
        receipt: Ino,
        parent_source_ino: Ino,
        name: &[u8],
        expected_ino: Ino,
        is_dir: bool,
    ) -> MetaResult<crate::trash::RetainedChildPurgeOutcome> {
        self.mutate(|local| {
            local.purge_retained_child(receipt, parent_source_ino, name, expected_ino, is_dir)
        })
    }
}

impl MetaVolumeSettings for PgMetaEngine {
    fn volume_settings(&self) -> MetaResult<VolumeSettings> {
        self.local.volume_settings()
    }

    fn compare_and_set_trash_days(
        &self,
        expected_revision: u64,
        days: u32,
    ) -> MetaResult<Option<VolumeSettings>> {
        self.change_mounted_settings(expected_revision, move |settings| {
            settings.trash.set_days(days)
        })
    }

    fn compare_and_set_logical_quota_bytes(
        &self,
        expected_revision: u64,
        bytes: Option<u64>,
    ) -> MetaResult<Option<VolumeSettings>> {
        self.change_mounted_settings(expected_revision, move |settings| {
            settings.logical_quota_bytes = bytes
        })
    }

    fn compare_and_set_compression(
        &self,
        expected_revision: u64,
        compression: crate::trash::CompressionPolicy,
    ) -> MetaResult<Option<VolumeSettings>> {
        self.change_mounted_settings(expected_revision, move |settings| {
            settings.compression = compression
        })
    }
}

impl PgMetaEngine {
    fn change_mounted_settings(
        &self,
        expected_revision: u64,
        update: impl FnOnce(&mut VolumeSettings) + Send + 'static,
    ) -> MetaResult<Option<VolumeSettings>> {
        self.ensure_remote_format_validated()?;
        let session = self.refresh_client_session()?;
        let pool = self.command_pool.clone();
        let timeout = self.command_timeout;
        let volume = self.volume_id;
        let client_id = self.current_client_id()?;
        let now_ms = Self::now_millis();
        let accepted = self.run_async(async move {
            Self::with_postgres_client_timeout(
                pool,
                timeout,
                "update volume settings",
                move |client| {
                    Box::pin(async move {
                        let tx = client.transaction().await.map_err(|error| {
                            Self::classify_pg_error("begin volume settings", error)
                        })?;
                        session.stamp(&tx, volume).await?;
                        let accepted = update_volume_settings_in_tx(
                            &tx,
                            volume,
                            Some(expected_revision),
                            &client_id,
                            now_ms,
                            update,
                        )
                        .await?;
                        if accepted.is_some() {
                            tx.commit().await.map_err(|error| {
                                Self::classify_pg_error("commit volume settings", error)
                            })?;
                        }
                        Ok(accepted)
                    })
                },
            )
            .await
        })?;
        if let Some(settings) = accepted {
            let events = [MetaEvent::SetVolumeSettings { settings }];
            self.local.apply_replica_events_to_base(&events)?;
            self.local.apply_replica_serving_state(&events)?;
        }
        Ok(accepted)
    }
}

/// Update the shared compression policy and publish its settings revision atomically.
pub async fn set_shared_compression(
    client: &mut tokio_postgres::Client,
    volume: i64,
    expected_revision: Option<u64>,
    compression: crate::trash::CompressionPolicy,
    client_id: &str,
    now_ms: i64,
) -> MetaResult<Option<VolumeSettings>> {
    update_volume_settings(
        client,
        volume,
        expected_revision,
        client_id,
        now_ms,
        |settings| settings.compression = compression,
    )
    .await
}

/// Apply the initial shared settings for a newly initialized volume in one transaction.
pub async fn set_shared_initial_volume_settings(
    client: &mut tokio_postgres::Client,
    volume: i64,
    compression: crate::trash::CompressionPolicy,
    trash_days: u32,
    client_id: &str,
    now_ms: i64,
) -> MetaResult<Option<VolumeSettings>> {
    update_volume_settings(client, volume, Some(0), client_id, now_ms, |settings| {
        settings.compression = compression;
        if trash_days > 0 {
            settings.trash.set_days(trash_days);
        }
    })
    .await
}

/// Update the shared logical byte limit and publish its settings revision atomically.
pub async fn set_shared_logical_quota_bytes(
    client: &mut tokio_postgres::Client,
    volume: i64,
    expected_revision: Option<u64>,
    bytes: Option<u64>,
    client_id: &str,
    now_ms: i64,
) -> MetaResult<Option<VolumeSettings>> {
    update_volume_settings(
        client,
        volume,
        expected_revision,
        client_id,
        now_ms,
        |settings| settings.logical_quota_bytes = bytes,
    )
    .await
}

/// Authoritative online trash-days update, shared by mounted engines and CLI.
/// `expected_revision` None updates whatever revision holds the row lock;
/// `Some` is a compare-and-set returning `None` when stale. Peers converge
/// through the ordered tag-19 event inserted in the same transaction.
pub async fn set_shared_trash_days(
    client: &mut tokio_postgres::Client,
    volume: i64,
    expected_revision: Option<u64>,
    days: u32,
    client_id: &str,
    now_ms: i64,
) -> MetaResult<Option<VolumeSettings>> {
    update_volume_settings(
        client,
        volume,
        expected_revision,
        client_id,
        now_ms,
        |settings| settings.trash.set_days(days),
    )
    .await
}

async fn update_volume_settings(
    client: &mut tokio_postgres::Client,
    volume: i64,
    expected_revision: Option<u64>,
    client_id: &str,
    now_ms: i64,
    update: impl FnOnce(&mut VolumeSettings),
) -> MetaResult<Option<VolumeSettings>> {
    let tx = client
        .transaction()
        .await
        .map_err(|error| PgMetaEngine::classify_pg_error("begin volume settings", error))?;
    let session = PublicationSession::begin(&tx, volume).await?;
    let accepted =
        update_volume_settings_in_tx(&tx, volume, expected_revision, client_id, now_ms, update)
            .await?;
    if accepted.is_some() {
        session.finish(&tx, volume).await?;
        tx.commit()
            .await
            .map_err(|error| PgMetaEngine::classify_pg_error("commit volume settings", error))?;
    }
    Ok(accepted)
}

async fn update_volume_settings_in_tx(
    tx: &tokio_postgres::Transaction<'_>,
    volume: i64,
    expected_revision: Option<u64>,
    client_id: &str,
    now_ms: i64,
    update: impl FnOnce(&mut VolumeSettings),
) -> MetaResult<Option<VolumeSettings>> {
    let row = tx
        .query_one(
            "SELECT metadata_format_version, volume_settings FROM metadata_state WHERE vol=$1 FOR UPDATE",
            &[&volume],
        )
        .await
        .map_err(|error| PgMetaEngine::classify_pg_error("lock volume settings", error))?;
    crate::ensure_supported_pg_sync_format_version(row.get(0))?;
    let bytes: Option<Vec<u8>> = row.get(1);
    let mut settings = bytes
        .as_deref()
        .map(serde_json::from_slice::<VolumeSettings>)
        .transpose()
        .map_err(|error| MetaError::Serialization(error.to_string()))?
        .unwrap_or_default();
    if expected_revision.is_some_and(|expected| settings.revision != expected) {
        return Ok(None);
    }
    settings.revision = settings
        .revision
        .checked_add(1)
        .ok_or_else(|| MetaError::Storage("volume settings revision overflow".into()))?;
    update(&mut settings);
    let event = MetaEvent::SetVolumeSettings { settings };
    let payload = crate::events::encode_event(&event)?;
    let payload_bytes = i64::try_from(payload.len())
        .map_err(|_| MetaError::Storage("volume settings payload too large".into()))?;
    let format = i16::try_from(crate::version::SHARED_VOLUME_FORMAT.current)
        .map_err(|_| MetaError::Storage("metadata format exceeds SMALLINT".into()))?;
    let row = tx
        .query_opt(
            sql::BUMP_METADATA_VERSION,
            &[
                &volume,
                &format,
                &payload_bytes,
                &super::volume_protocol::PUBLICATION_WRITE_REQUIREMENT,
            ],
        )
        .await
        .map_err(|error| PgMetaEngine::classify_pg_error("order volume settings", error))?;
    let Some(row) = row else {
        return Err(super::volume_protocol::refused_publication(tx, volume).await);
    };
    let version: i64 = row.get(0);
    tx.execute(
        sql::INSERT_EVENT_LOG_BATCH,
        &[
            &volume,
            &version,
            &client_id,
            &now_ms,
            &vec![event.kind()],
            &vec![payload],
        ],
    )
    .await
    .map_err(|error| PgMetaEngine::classify_pg_error("publish volume settings", error))?;
    let bytes = serde_json::to_vec(&settings)
        .map_err(|error| MetaError::Serialization(error.to_string()))?;
    tx.execute(
        "UPDATE metadata_state SET volume_settings=$2 WHERE vol=$1",
        &[&volume, &bytes],
    )
    .await
    .map_err(|error| PgMetaEngine::classify_pg_error("store volume settings", error))?;
    tx.execute(
        "SELECT pg_notify($1, $2)",
        &[
            &PgMetaEngine::metadata_notify_channel(volume),
            &version.to_string(),
        ],
    )
    .await
    .map_err(|error| PgMetaEngine::classify_pg_error("notify volume settings", error))?;
    Ok(Some(settings))
}

/// Atomically updates mutable object-store settings and, when requested, trash
/// retention. Immutable volume identity and encryption fields come from the
/// locked shared FormatInfo and are never supplied by the caller.
pub async fn update_shared_s3_settings(
    client: &mut tokio_postgres::Client,
    volume: i64,
    expected_uuid: &str,
    update: S3VolumeSettingsUpdate<'_>,
    client_id: &str,
    now_ms: i64,
) -> MetaResult<crate::FormatInfo> {
    let tx = client
        .transaction()
        .await
        .map_err(|error| PgMetaEngine::classify_pg_error("begin shared volume update", error))?;
    let session = PublicationSession::begin(&tx, volume).await?;
    let row = tx
        .query_one(
            "SELECT ms.metadata_format_version, ms.format_info, v.uuid FROM metadata_state ms JOIN volumes v ON v.vol=ms.vol WHERE ms.vol=$1 FOR UPDATE OF ms",
            &[&volume],
        )
        .await
        .map_err(|error| PgMetaEngine::classify_pg_error("lock shared volume update", error))?;
    crate::ensure_supported_pg_sync_format_version(row.get(0))?;
    let uuid: String = row.get(2);
    if uuid != expected_uuid {
        return Err(MetaError::Storage("shared volume identity mismatch".into()));
    }
    let current: Vec<u8> = row.get(1);
    let mut format = crate::decode_format_info(&current)?;
    if format.uuid != expected_uuid {
        return Err(MetaError::Storage("shared format identity mismatch".into()));
    }
    if let Some(value) = update.endpoint {
        format.endpoint = value.to_string();
    }
    if let Some(value) = update.bucket {
        format.bucket = value.to_string();
    }
    if let Some(value) = update.access_key {
        format.access_key = value.to_string();
    }
    if let Some(value) = update.secret_key {
        format.secret_key = value.to_string();
    }
    if let Some(value) = update.region {
        format.region = value.to_string();
    }
    let format_bytes =
        serde_json::to_vec(&format).map_err(|error| MetaError::Serialization(error.to_string()))?;
    tx.execute(
        "UPDATE metadata_state SET format_info=$2 WHERE vol=$1",
        &[&volume, &format_bytes],
    )
    .await
    .map_err(|error| PgMetaEngine::classify_pg_error("store shared S3 settings", error))?;
    if let Some(days) = update.trash_days {
        update_volume_settings_in_tx(&tx, volume, None, client_id, now_ms, |settings| {
            settings.trash.set_days(days)
        })
        .await?
        .ok_or_else(|| MetaError::Storage("shared trash settings update was rejected".into()))?;
    }
    session.finish(&tx, volume).await?;
    tx.commit()
        .await
        .map_err(|error| PgMetaEngine::classify_pg_error("commit shared volume update", error))?;
    Ok(format)
}

use super::*;
use crate::MetaEvent;
use crate::trash::{MetaVolumeSettings, VolumeSettings};

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
        let pool = self.command_pool.clone();
        let timeout = self.command_timeout;
        let volume = self.volume_id;
        let client_id = self.current_client_id()?;
        let now_ms = PgMetaEngine::now_millis();
        let accepted = self.run_async(async move {
            Self::with_postgres_client_timeout(
                pool,
                timeout,
                "update volume settings",
                move |client| {
                    Box::pin(async move {
                        set_shared_trash_days(
                            client,
                            volume,
                            Some(expected_revision),
                            days,
                            &client_id,
                            now_ms,
                        )
                        .await
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
    let tx = client
        .transaction()
        .await
        .map_err(|error| PgMetaEngine::classify_pg_error("begin volume settings", error))?;
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
    settings.trash.set_days(days);
    let event = MetaEvent::SetVolumeSettings { settings };
    let payload = crate::events::encode_event(&event)?;
    let payload_bytes = i64::try_from(payload.len())
        .map_err(|_| MetaError::Storage("volume settings payload too large".into()))?;
    let format = i16::try_from(CURRENT_FORMAT_VERSION)
        .map_err(|_| MetaError::Storage("metadata format exceeds SMALLINT".into()))?;
    let row = tx
        .query_one(
            sql::BUMP_METADATA_VERSION,
            &[&volume, &format, &payload_bytes],
        )
        .await
        .map_err(|error| PgMetaEngine::classify_pg_error("order volume settings", error))?;
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
    tx.commit()
        .await
        .map_err(|error| PgMetaEngine::classify_pg_error("commit volume settings", error))?;
    Ok(Some(settings))
}

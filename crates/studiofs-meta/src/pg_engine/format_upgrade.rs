use super::*;

/// Upgrade shared metadata while the calling volume is stopped.
///
/// Existing checkpoint bytes, event history and payload references are retained.
/// The caller upgrades its local header only after this transaction commits;
/// retry after a lost response is safe. Outstanding deletion owners must settle
/// before retry, including owners whose leases expired: expiry does not prove
/// an old object-store DELETE finished.
pub async fn upgrade_shared_format5(
    client: &mut tokio_postgres::Client,
    volume: i64,
    expected_uuid: &str,
) -> MetaResult<()> {
    upgrade_shared_step(client, volume, expected_uuid, 5, 6)
        .await
        .map(|_| ())
}

/// Adopt a format-6 volume into the current baseline, or return its committed header.
pub async fn upgrade_shared_format6(
    client: &mut tokio_postgres::Client,
    volume: i64,
    expected_uuid: &str,
) -> MetaResult<FormatInfo> {
    upgrade_shared_step(
        client,
        volume,
        expected_uuid,
        6,
        crate::version::SHARED_VOLUME_FORMAT.current_pg_smallint(),
    )
    .await
}

async fn upgrade_shared_step(
    client: &mut tokio_postgres::Client,
    volume: i64,
    expected_uuid: &str,
    from: i16,
    to: i16,
) -> MetaResult<FormatInfo> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let tx = client.transaction().await
            .map_err(|error| PgMetaEngine::classify_pg_error("begin format upgrade", error))?;
        tx.batch_execute("SET LOCAL lock_timeout = '2s'").await
            .map_err(|error| PgMetaEngine::classify_pg_error("bound format upgrade locks", error))?;
        tx.execute("SELECT pg_advisory_xact_lock($1)", &[&super::migrations::SCHEMA_ADVISORY_LOCK]).await
            .map_err(|error| PgMetaEngine::classify_pg_error("lock schema for format upgrade", error))?;
        let lock = tx.query_one("SELECT pg_try_advisory_xact_lock($1)",
            &[&super::janitor::pg_janitor_lock_key(volume)]).await
            .map_err(|error| PgMetaEngine::classify_pg_error("lock format upgrade janitor", error))?;
        if !lock.get::<_, bool>(0) {
            return Err(MetaError::Storage("stop the active janitor before upgrading".into()));
        }
        let row = tx.query_one(
            "SELECT ms.metadata_format_version, ms.format_info, v.uuid FROM metadata_state ms JOIN volumes v ON v.vol=ms.vol WHERE ms.vol=$1 FOR UPDATE OF ms",
            &[&volume],
        ).await.map_err(|error| PgMetaEngine::classify_pg_error("lock format upgrade volume", error))?;
        let version: i16 = row.get(0);
        let uuid: String = row.get(2);
        if uuid != expected_uuid {
            return Err(MetaError::Storage("format upgrade volume identity mismatch".into()));
        }
        if version != from && version != to && !(from == 5 && version == crate::version::SHARED_VOLUME_FORMAT.current_pg_smallint()) {
            return Err(MetaError::UnsupportedFormatVersion { found: version as u16, expected: to as u16 });
        }
        let bytes: Vec<u8> = row.get(1);
        let mut format: FormatInfo = serde_json::from_slice(&bytes)
            .map_err(crate::format_info_decode_error)?;
        if format.format_version != version as u16 || format.uuid != expected_uuid {
            return Err(MetaError::Storage("format upgrade header identity mismatch".into()));
        }
        format.validate_format_contract()?;
        if version != from {
            return Ok(format);
        }
        let row = tx.query_one(
            "SELECT EXISTS(SELECT 1 FROM snapshot_payload_upload_objects WHERE vol=$1 AND reconcile_owner IS NOT NULL), EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='snapshot_payload_upload_objects'::regclass AND tgname='studiofs_legacy_deletion_claim_fence' AND tgenabled IN ('O','A'))",
            &[&volume],
        ).await.map_err(|error| PgMetaEngine::classify_pg_error("check format upgrade deletion fences", error))?;
        if row.get::<_, bool>(0) || !row.get::<_, bool>(1) {
            return Err(MetaError::Storage("format upgrade requires settled deletion owners and the legacy-claim guard".into()));
        }
        if from == 6 {
            let (_, pending) = super::migrations::registered_schema_status(&tx, false).await?;
            if !pending.is_empty() {
                return Err(MetaError::Storage("apply pending schema migrations before adopting the volume".into()));
            }
            super::insert_initial_volume_protocol(&tx, volume, "studiofs upgrade").await?;
        }
        let session = super::client_sessions::PublicationSession::begin(&tx, volume).await?;
        format.format_version = to as u16;
        let bytes = serde_json::to_vec(&format)
            .map_err(|error| MetaError::Serialization(error.to_string()))?;
        tx.execute("UPDATE metadata_state SET metadata_format_version=$3, format_info=$2 WHERE vol=$1", &[&volume, &bytes, &to]).await
            .map_err(|error| PgMetaEngine::classify_pg_error("upgrade shared format", error))?;
        session.finish(&tx, volume).await?;
        tx.commit().await.map_err(|error| PgMetaEngine::classify_pg_error("commit format upgrade", error))?;
        Ok(format)
    }).await.map_err(|_| MetaError::Storage("format upgrade timed out".into()))?
}

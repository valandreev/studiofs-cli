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
    tokio::time::timeout(Duration::from_secs(30), async {
        let tx = client.transaction().await
            .map_err(|error| PgMetaEngine::classify_pg_error("begin format upgrade", error))?;
        tx.batch_execute("SET LOCAL lock_timeout = '2s'").await
            .map_err(|error| PgMetaEngine::classify_pg_error("bound format upgrade locks", error))?;
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
        if version == 6 { return Ok(()); }
        if version != 5 {
            return Err(MetaError::UnsupportedFormatVersion { found: version as u16, expected: 5 });
        }
        let row = tx.query_one(
            "SELECT EXISTS(SELECT 1 FROM snapshot_payload_upload_objects WHERE vol=$1 AND reconcile_owner IS NOT NULL), EXISTS(SELECT 1 FROM pg_trigger WHERE tgname='studiofs_legacy_deletion_claim_fence' AND tgenabled IN ('O','A'))",
            &[&volume],
        ).await.map_err(|error| PgMetaEngine::classify_pg_error("check format upgrade deletion fences", error))?;
        if row.get::<_, bool>(0) || !row.get::<_, bool>(1) {
            return Err(MetaError::Storage("format upgrade requires settled deletion owners and the legacy-claim guard".into()));
        }
        let row = tx.query_one("SELECT format_info FROM metadata_state WHERE vol=$1", &[&volume]).await
            .map_err(|error| PgMetaEngine::classify_pg_error("read upgrade format", error))?;
        let bytes: Vec<u8> = row.get(0);
        let mut format: FormatInfo = serde_json::from_slice(&bytes)
            .map_err(|error| MetaError::Serialization(error.to_string()))?;
        if format.format_version != 5 || format.uuid != expected_uuid {
            return Err(MetaError::Storage("format upgrade header identity mismatch".into()));
        }
        format.validate_format_contract()?;
        format.format_version = 6;
        let bytes = serde_json::to_vec(&format)
            .map_err(|error| MetaError::Serialization(error.to_string()))?;
        tx.execute("UPDATE metadata_state SET metadata_format_version=6, format_info=$2 WHERE vol=$1", &[&volume, &bytes]).await
            .map_err(|error| PgMetaEngine::classify_pg_error("upgrade shared format", error))?;
        tx.commit().await.map_err(|error| PgMetaEngine::classify_pg_error("commit format upgrade", error))
    }).await.map_err(|_| MetaError::Storage("format upgrade timed out".into()))?
}

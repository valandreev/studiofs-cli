use super::{PgMetaEngine, sql};
use crate::engine::{MetaError, MetaResult};
use std::sync::atomic::Ordering;

impl PgMetaEngine {
    pub(super) fn pull_snapshot_locked(&self) -> MetaResult<bool> {
        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;
        let local_version = self.local_version.load(Ordering::SeqCst);
        let connect_timeout = self.connect_timeout;
        let command_timeout = self.command_timeout;

        let row = self.run_async(async move {
            Self::with_postgres_client_timeout(
                postgres_url,
                connect_timeout,
                command_timeout,
                "pull snapshot",
                move |client| async move {
                    let version_row = client
                        .query_opt(sql::SELECT_METADATA_VERSION, &[&volume_id])
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres snapshot version select failed: {e}"
                            ))
                        })?;

                    let Some(version_row) = version_row else {
                        return Ok(None);
                    };
                    let version: i64 = version_row.get(0);
                    if version <= local_version {
                        return Ok(None);
                    }

                    let row = client
                        .query_one(sql::SELECT_METADATA_SNAPSHOT, &[&volume_id])
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres snapshot payload select failed: {e}"
                            ))
                        })?;

                    Ok(Some(row))
                },
            )
            .await
        })?;

        let Some(row) = row else {
            return Ok(false);
        };

        let version: i64 = row.get(0);
        let bytes: Vec<u8> = row.get(1);

        let snapshot = Self::decode_snapshot(&bytes)?;
        let replay_start_version =
            Self::snapshot_replay_start_version(&snapshot, version, local_version);
        self.local.import_remote_snapshot(&snapshot)?;
        self.reconcile_imported_snapshot_regular_files()?;
        self.persist_local_version(replay_start_version)?;
        Ok(true)
    }

    pub(super) fn remote_metadata_head_version(&self) -> MetaResult<i64> {
        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;
        let connect_timeout = self.connect_timeout;
        let command_timeout = self.command_timeout;

        self.run_async(async move {
            Self::with_postgres_client_timeout(
                postgres_url,
                connect_timeout,
                command_timeout,
                "metadata head version",
                move |client| async move {
                    let row = client
                        .query_opt(sql::SELECT_METADATA_VERSION, &[&volume_id])
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres metadata head version select failed: {e}"
                            ))
                        })?;
                    Ok(row.map(|row| row.get(0)).unwrap_or(0))
                },
            )
            .await
        })
    }

    pub(super) async fn query_foreign_event_rows<C>(
        client: &C,
        volume_id: i64,
        last_seq: i64,
    ) -> MetaResult<Vec<tokio_postgres::Row>>
    where
        C: tokio_postgres::GenericClient + Sync,
    {
        client
            .query(sql::SELECT_FOREIGN_EVENT_ROWS, &[&volume_id, &last_seq])
            .await
            .map_err(|e| {
                MetaError::Storage(format!(
                    "postgres event_log select failed: {}",
                    Self::format_pg_error(e)
                ))
            })
    }

    pub(super) fn write_checkpoint_locked(&self) -> MetaResult<()> {
        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;
        let snapshot = self.local.export_remote_snapshot()?;
        let payload = serde_json::to_vec(&snapshot)
            .map_err(|e| MetaError::Serialization(format!("snapshot encode failed: {e}")))?;
        let version = self.local_version.load(Ordering::SeqCst);
        let connect_timeout = self.connect_timeout;
        let command_timeout = self.command_timeout;

        self.run_async(async move {
            Self::with_postgres_client_timeout(
                postgres_url,
                connect_timeout,
                command_timeout,
                "write checkpoint",
                move |client| async move {
                    client
                        .execute(
                            sql::UPSERT_METADATA_CHECKPOINT,
                            &[&volume_id, &version, &payload],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!("postgres checkpoint upsert failed: {e}"))
                        })?;

                    Ok(())
                },
            )
            .await
        })
    }
}

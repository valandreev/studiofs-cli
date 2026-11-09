use super::{PgMetaEngine, PgTimeoutContext};
use crate::engine::{MetaError, MetaResult};
use crate::janitor::{JanitorMetadataError, JanitorSweepCheckpoint, JanitorSweepScope};
use crate::pg_transport::connect_studiofs_postgres;
use crate::schema::FormatInfo;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio_postgres::Client;

const PG_JANITOR_LOCK_SALT: i64 = 0x5f53_4653_5f4a_414e;

/// Objects retained by live or leased snapshots; shared by mounted and CLI janitors.
pub(super) const SNAPSHOT_PIN_QUERY: &str = "SELECT DISTINCT reachability.object_id
    FROM snapshot_object_reachability AS reachability
    JOIN snapshot_catalog AS catalog
      ON catalog.vol = reachability.vol
     AND catalog.cut_version >= reachability.start_version
     AND (reachability.end_version IS NULL
          OR catalog.cut_version < reachability.end_version)
    WHERE reachability.vol = $1
      AND (
          catalog.lifecycle IN ('creating', 'ready')
          OR (
              catalog.lifecycle = 'deleting'
              AND EXISTS (
                  SELECT 1
                  FROM snapshot_mount_leases AS mount_lease
                  WHERE mount_lease.vol = catalog.vol
                    AND mount_lease.snapshot_id = catalog.id
                    AND mount_lease.released_at IS NULL
                    AND mount_lease.lease_expires_at > clock_timestamp()
              )
          )
      )";

pub(crate) struct PgJanitorSession {
    postgres_url: String,
    volume_id: i64,
    connect_timeout: Duration,
    command_timeout: Duration,
    runtime: Option<Runtime>,
    client: Option<Client>,
    connection_task: Option<JoinHandle<()>>,
}

pub(crate) struct PgJanitorProtection {
    pub(crate) referenced: HashSet<u64>,
    pub(crate) snapshot_pins: HashSet<u64>,
    pub(crate) gc_candidates: HashMap<u64, (i64, bool)>,
}

pub(crate) const PG_JANITOR_CANDIDATE_PAGE_SIZE: usize = 64;

fn canonical_sweep_object_id(prefix: &str, key: &str) -> Option<u64> {
    let suffix = key.strip_prefix(prefix)?;
    let mut parts = suffix.split('/');
    let shard = parts.next()?;
    let object_id = parts.next()?;
    let canonical_hex = |part: &str, length| {
        part.len() == length
            && part
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    };
    if !canonical_hex(shard, 4) || !canonical_hex(object_id, 16) {
        return None;
    }
    let object_id = u64::from_str_radix(object_id, 16).ok()?;
    if u16::from_str_radix(shard, 16).ok()? != object_id as u16 {
        return None;
    }
    match (parts.next(), parts.next()) {
        (None, None) => Some(object_id),
        (Some("0"), None) => Some(object_id),
        _ => None,
    }
}

fn safe_sweep_key(prefix: &str, key: &str) -> bool {
    let Some(suffix) = key.strip_prefix(prefix) else {
        return false;
    };
    !suffix.is_empty()
        && key.len() <= 1024
        && !key.bytes().any(|byte| byte.is_ascii_control())
        && !suffix.split('/').any(|part| matches!(part, "." | ".."))
}

fn pg_sweep_timeout(timeout: Duration) -> JanitorMetadataError {
    JanitorMetadataError::BackendUnavailable(format!(
        "PostgreSQL janitor sweep checkpoint timed out after {} seconds",
        timeout.as_secs()
    ))
}

async fn persist_sweep_checkpoint(
    client: &Client,
    volume_id: i64,
    scope: &JanitorSweepScope<'_>,
    checkpoint: &JanitorSweepCheckpoint,
) -> Result<(), JanitorMetadataError> {
    if !(1..=20).contains(&checkpoint.next_prefix.len())
        || !checkpoint
            .next_prefix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || checkpoint.pending_object_id.is_some() != checkpoint.pending_key.is_some()
        || (checkpoint.pending_key.is_some() && checkpoint.after_key.is_none())
        || checkpoint.after_key.as_deref().is_some_and(|key| {
            !safe_sweep_key(scope.data_prefix, key)
                || checkpoint
                    .pending_key
                    .as_deref()
                    .is_some_and(|pending_key| key < pending_key)
        })
        || checkpoint.pending_key.as_deref().is_some_and(|key| {
            !safe_sweep_key(scope.data_prefix, key)
                || checkpoint.pending_object_id.is_some_and(|object_id| {
                    canonical_sweep_object_id(scope.data_prefix, key) != Some(object_id)
                })
        })
    {
        return Err(JanitorMetadataError::UnsafeToDelete(
            "janitor sweep checkpoint position is invalid",
        ));
    }
    let pending_object_id = checkpoint
        .pending_object_id
        .map(i64::try_from)
        .transpose()
        .map_err(|_| {
            JanitorMetadataError::UnsafeToDelete("janitor sweep checkpoint object id is invalid")
        })?;
    let affected = client
        .execute(
            "INSERT INTO snapshot_payload_sweep_checkpoints (vol, schema_version, volume_uuid, endpoint, bucket, data_prefix, listing_strategy, after_key, pending_object_id, pending_key, next_prefix) VALUES ($1, 1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (vol) DO UPDATE SET schema_version = EXCLUDED.schema_version, volume_uuid = EXCLUDED.volume_uuid, endpoint = EXCLUDED.endpoint, bucket = EXCLUDED.bucket, data_prefix = EXCLUDED.data_prefix, listing_strategy = EXCLUDED.listing_strategy, after_key = EXCLUDED.after_key, pending_object_id = EXCLUDED.pending_object_id, pending_key = EXCLUDED.pending_key, next_prefix = EXCLUDED.next_prefix WHERE snapshot_payload_sweep_checkpoints.schema_version = 1",
            &[
                &volume_id,
                &scope.volume_uuid,
                &scope.endpoint,
                &scope.bucket,
                &scope.data_prefix,
                &scope.listing_strategy,
                &checkpoint.after_key,
                &pending_object_id,
                &checkpoint.pending_key,
                &checkpoint.next_prefix,
            ],
        )
        .await
        .map_err(|error| janitor_pg_error("janitor sweep checkpoint write failed", error))?;
    if affected != 1 {
        return Err(JanitorMetadataError::UnsafeToDelete(
            "janitor sweep checkpoint schema is unsupported",
        ));
    }
    Ok(())
}

impl PgJanitorSession {
    pub(crate) fn new(postgres_url: String, volume_id: i64) -> Self {
        Self {
            postgres_url,
            volume_id,
            connect_timeout: PgMetaEngine::default_connect_timeout(),
            command_timeout: PgMetaEngine::default_command_timeout(),
            runtime: None,
            client: None,
            connection_task: None,
        }
    }

    pub(crate) fn lock_is_held(&self) -> bool {
        self.client.is_some()
    }

    pub(crate) fn load_sweep_checkpoint(
        &mut self,
        scope: JanitorSweepScope<'_>,
        persist_initial: bool,
    ) -> Result<JanitorSweepCheckpoint, JanitorMetadataError> {
        let (Some(client), Some(runtime)) = (self.client.as_mut(), self.runtime.as_ref()) else {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "PostgreSQL janitor lock is not held",
            ));
        };
        let volume_id = self.volume_id;
        let timeout = self.command_timeout;
        let result = runtime.block_on(async {
            tokio::time::timeout(timeout, async {
                let row = client.query_opt(
                    "SELECT schema_version, volume_uuid, endpoint, bucket, data_prefix, listing_strategy, after_key, pending_object_id, pending_key, next_prefix FROM snapshot_payload_sweep_checkpoints WHERE vol = $1",
                    &[&volume_id],
                ).await.map_err(|error| janitor_pg_error("janitor sweep checkpoint read failed", error))?;
                let Some(row) = row else {
                    let checkpoint = JanitorSweepCheckpoint { after_key: None, pending_object_id: None, pending_key: None, next_prefix: "0".into() };
                    if persist_initial { persist_sweep_checkpoint(client, volume_id, &scope, &checkpoint).await?; }
                    return Ok(checkpoint);
                };
                let version: i16 = row.get(0);
                if version != 1 {
                    return Err(JanitorMetadataError::UnsafeToDelete("janitor sweep checkpoint schema is unsupported"));
                }
                let stored_scope = (
                    row.get::<usize, String>(1), row.get::<usize, String>(2),
                    row.get::<usize, String>(3), row.get::<usize, String>(4),
                    row.get::<usize, String>(5),
                );
                if stored_scope.0 == scope.volume_uuid && stored_scope.1 == scope.endpoint
                    && stored_scope.2 == scope.bucket && stored_scope.3 == scope.data_prefix
                    && stored_scope.4 == scope.listing_strategy
                {
                    let after_key: Option<String> = row.get(6);
                    let pending_object_id: Option<i64> = row.get(7);
                    let pending_object_id = pending_object_id
                        .map(u64::try_from)
                        .transpose()
                        .map_err(|_| JanitorMetadataError::UnsafeToDelete("janitor sweep checkpoint object id is invalid"))?;
                    let pending_key: Option<String> = row.get(8);
                    let next_prefix: String = row.get(9);
                    if !next_prefix.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                        || !(1..=20).contains(&next_prefix.len())
                        || pending_object_id.is_some() != pending_key.is_some()
                        || (pending_key.is_some() && after_key.is_none())
                        || after_key.as_deref().is_some_and(|key| {
                            !safe_sweep_key(scope.data_prefix, key)
                                || pending_key.as_deref().is_some_and(|pending_key| key < pending_key)
                        })
                        || pending_key.as_deref().is_some_and(|key| {
                            !safe_sweep_key(scope.data_prefix, key)
                                || pending_object_id.is_some_and(|object_id| {
                                    canonical_sweep_object_id(scope.data_prefix, key) != Some(object_id)
                                })
                        })
                    {
                        return Err(JanitorMetadataError::UnsafeToDelete("janitor sweep checkpoint position is invalid"));
                    }
                    return Ok(JanitorSweepCheckpoint { after_key, pending_object_id, pending_key, next_prefix });
                }
                let checkpoint = JanitorSweepCheckpoint { after_key: None, pending_object_id: None, pending_key: None, next_prefix: "0".into() };
                if persist_initial {
                    persist_sweep_checkpoint(client, volume_id, &scope, &checkpoint).await?;
                }
                Ok(checkpoint)
            }).await
        });
        result.map_err(|_| pg_sweep_timeout(timeout))?
    }

    pub(crate) fn save_sweep_checkpoint(
        &mut self,
        scope: JanitorSweepScope<'_>,
        checkpoint: &JanitorSweepCheckpoint,
    ) -> Result<(), JanitorMetadataError> {
        let (Some(client), Some(runtime)) = (self.client.as_mut(), self.runtime.as_ref()) else {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "PostgreSQL janitor lock is not held",
            ));
        };
        let timeout = self.command_timeout;
        let volume_id = self.volume_id;
        runtime
            .block_on(async {
                tokio::time::timeout(
                    timeout,
                    persist_sweep_checkpoint(client, volume_id, &scope, checkpoint),
                )
                .await
            })
            .map_err(|_| pg_sweep_timeout(timeout))?
    }

    pub(crate) fn reset_sweep_checkpoint(
        &mut self,
        _scope: JanitorSweepScope<'_>,
    ) -> Result<(), JanitorMetadataError> {
        let (Some(client), Some(runtime)) = (self.client.as_mut(), self.runtime.as_ref()) else {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "PostgreSQL janitor lock is not held",
            ));
        };
        let timeout = self.command_timeout;
        let volume_id = self.volume_id;
        runtime.block_on(async {
            tokio::time::timeout(timeout, async {
                if let Some(row) = client
                    .query_opt(
                        "SELECT schema_version FROM snapshot_payload_sweep_checkpoints WHERE vol = $1",
                        &[&volume_id],
                    )
                    .await
                    .map_err(|error| janitor_pg_error("janitor sweep checkpoint read failed", error))?
                {
                    let version: i16 = row.get(0);
                    if version != 1 {
                        return Err(JanitorMetadataError::UnsafeToDelete(
                            "janitor sweep checkpoint schema is unsupported",
                        ));
                    }
                    client
                        .execute(
                            "DELETE FROM snapshot_payload_sweep_checkpoints WHERE vol = $1 AND schema_version = 1",
                            &[&volume_id],
                        )
                        .await
                        .map_err(|error| janitor_pg_error("janitor sweep checkpoint reset failed", error))?;
                }
                Ok(())
            })
            .await
        })
        .map_err(|_| pg_sweep_timeout(timeout))?
    }

    pub(crate) fn try_acquire_global_janitor_lock(&mut self) -> MetaResult<bool> {
        if self.client.is_some() {
            return Ok(false);
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|error| {
                MetaError::Storage(format!("failed to build janitor runtime: {error}"))
            })?;
        let connect = connect_studiofs_postgres(&self.postgres_url);
        let (client, connection) = runtime
            .block_on(async { tokio::time::timeout(self.connect_timeout, connect).await })
            .map_err(|_| {
                PgMetaEngine::pg_timeout_error(
                    PgTimeoutContext::Connection("janitor lock connect"),
                    self.connect_timeout,
                )
            })??;
        let connection_task = runtime.spawn(async move {
            let _ = connection.await;
        });
        self.runtime = Some(runtime);
        self.client = Some(client);
        self.connection_task = Some(connection_task);

        let lock_key = pg_janitor_lock_key(self.volume_id);
        let result = match (self.runtime.as_ref(), self.client.as_ref()) {
            (Some(runtime), Some(client)) => runtime.block_on(async {
                tokio::time::timeout(
                    self.command_timeout,
                    client.query_one("SELECT pg_try_advisory_lock($1)", &[&lock_key]),
                )
                .await
            }),
            _ => {
                self.close_connection();
                return Err(MetaError::Storage(
                    "janitor PostgreSQL session was not initialized".to_owned(),
                ));
            }
        };
        match result {
            Ok(Ok(row)) => {
                let acquired = row.get::<usize, bool>(0);
                if !acquired {
                    self.close_connection();
                }
                Ok(acquired)
            }
            Ok(Err(error)) => {
                self.close_connection();
                Err(PgMetaEngine::classify_pg_error(
                    "postgres janitor lock acquire failed",
                    error,
                ))
            }
            Err(_) => {
                self.close_connection();
                Err(PgMetaEngine::pg_timeout_error(
                    PgTimeoutContext::Command("janitor lock acquire"),
                    self.command_timeout,
                ))
            }
        }
    }

    pub(crate) fn release_global_janitor_lock(&mut self) -> MetaResult<()> {
        if self.client.is_none() {
            return Ok(());
        }
        let lock_key = pg_janitor_lock_key(self.volume_id);
        let result = match (self.runtime.as_ref(), self.client.as_ref()) {
            (Some(runtime), Some(client)) => runtime.block_on(async {
                tokio::time::timeout(
                    self.command_timeout,
                    client.query_one("SELECT pg_advisory_unlock($1)", &[&lock_key]),
                )
                .await
            }),
            _ => {
                self.close_connection();
                return Err(MetaError::Storage(
                    "janitor PostgreSQL session was not initialized".to_owned(),
                ));
            }
        };
        self.close_connection();
        match result {
            Ok(Ok(row)) if row.get::<usize, bool>(0) => Ok(()),
            Ok(Ok(_)) => Err(MetaError::Storage(
                "postgres janitor lock was not held during release".to_owned(),
            )),
            Ok(Err(error)) => Err(PgMetaEngine::classify_pg_error(
                "postgres janitor lock release failed",
                error,
            )),
            Err(_) => Err(PgMetaEngine::pg_timeout_error(
                PgTimeoutContext::Command("janitor lock release"),
                self.command_timeout,
            )),
        }
    }

    fn close_connection(&mut self) {
        drop(self.client.take());
        if let Some(task) = self.connection_task.take() {
            task.abort();
            if let Some(runtime) = self.runtime.as_ref() {
                let _ = runtime.block_on(task);
            }
        }
        self.runtime.take();
    }

    pub(crate) fn read_protection(
        &mut self,
        local_info: &FormatInfo,
        local_version: u64,
        object_ids: &[u64],
        eligible_before: i64,
    ) -> Result<PgJanitorProtection, JanitorMetadataError> {
        let (Some(client), Some(runtime)) = (self.client.as_mut(), self.runtime.as_ref()) else {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "PostgreSQL janitor lock is not held",
            ));
        };
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;
        let local_info = local_info.clone();
        let object_ids = object_ids
            .iter()
            .map(|id| {
                i64::try_from(*id).map_err(|_| {
                    JanitorMetadataError::UnsafeToDelete("janitor object id is invalid")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let result = runtime.block_on(async {
            tokio::time::timeout(command_timeout, async {
                let transaction = client.transaction().await.map_err(|error| {
                    janitor_pg_error("janitor metadata transaction failed", error)
                })?;
                transaction
                    .batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                    .await
                    .map_err(|error| {
                        janitor_pg_error("janitor read-only transaction failed", error)
                    })?;

                let row = transaction
                    .query_opt(
                        "SELECT volumes.uuid, metadata_state.version, metadata_state.metadata_format_version, metadata_state.format_info, metadata_state.checkpoint_publication_state, metadata_state.checkpoint_generation FROM volumes INNER JOIN metadata_state ON metadata_state.vol = volumes.vol WHERE volumes.vol = $1",
                        &[&volume_id],
                    )
                    .await
                    .map_err(|error| {
                        janitor_pg_error("janitor volume identity query failed", error)
                    })?
                    .ok_or(JanitorMetadataError::UnsafeToDelete(
                        "PostgreSQL volume binding is missing",
                    ))?;

                let remote_uuid: String = row.get(0);
                let remote_head: i64 = row.get(1);
                let metadata_format_version: Option<i16> = row.get(2);
                let remote_format_bytes: Vec<u8> = row.get(3);
                let publication_state: String = row.get(4);
                let generation: i64 = row.get(5);
                if remote_head < 0
                    || publication_state != "published"
                    || generation <= 0
                    || remote_format_bytes.is_empty()
                {
                    return Err(JanitorMetadataError::UnsafeToDelete(
                        "PostgreSQL storage binding is incomplete",
                    ));
                }
                crate::ensure_supported_pg_sync_format_version(metadata_format_version)
                    .map_err(|_| {
                        JanitorMetadataError::UnsafeToDelete("PostgreSQL format is unsupported")
                    })?;
                let remote_info = crate::decode_format_info(&remote_format_bytes).map_err(|_| {
                    JanitorMetadataError::UnsafeToDelete("PostgreSQL storage binding is invalid")
                })?;
                if remote_uuid != local_info.uuid
                    || remote_info.name != local_info.name
                    || remote_info.uuid != local_info.uuid
                    || remote_info.endpoint != local_info.endpoint
                    || remote_info.bucket != local_info.bucket
                    || remote_info.data_prefix != local_info.data_prefix
                    || remote_info.metadata.backend != crate::schema::MetadataBackend::Postgres
                    || remote_info
                        .metadata
                        .postgres
                        .as_ref()
                        .is_none_or(|config| config.volume_id != volume_id)
                {
                    return Err(JanitorMetadataError::UnsafeToDelete(
                        "PostgreSQL volume identity or storage binding does not match",
                    ));
                }
                if i64::try_from(local_version).ok() != Some(remote_head) {
                    return Err(JanitorMetadataError::UnsafeToDelete(
                        "local metadata is not synchronized with PostgreSQL",
                    ));
                }

                let bootstrap = transaction
                    .query_opt(
                        "SELECT initialized_version FROM snapshot_reachability_bootstrap WHERE vol = $1",
                        &[&volume_id],
                    )
                    .await
                    .map_err(|error| {
                        janitor_pg_error("janitor snapshot bootstrap query failed", error)
                    })?;
                let Some(bootstrap) = bootstrap else {
                    return Err(JanitorMetadataError::UnsafeToDelete(
                        "snapshot reachability bootstrap is incomplete",
                    ));
                };
                let initialized_version: i64 = bootstrap.get(0);
                if !(0..=remote_head).contains(&initialized_version) {
                    return Err(JanitorMetadataError::UnsafeToDelete(
                        "snapshot reachability bootstrap is incomplete",
                    ));
                }
                let reference_rows = transaction
                    .query(
                        "SELECT DISTINCT object_id FROM snapshot_current_references WHERE vol = $1 AND object_id = ANY($2::BIGINT[])",
                        &[&volume_id, &object_ids],
                    )
                    .await
                    .map_err(|error| {
                        janitor_pg_error("janitor current-reference query failed", error)
                    })?;
                let pin_rows = transaction
                    .query(
                        &format!("{SNAPSHOT_PIN_QUERY} AND reachability.object_id = ANY($2::BIGINT[])"),
                        &[&volume_id, &object_ids],
                    )
                    .await
                    .map_err(|error| {
                        janitor_pg_error("janitor snapshot pin query failed", error)
                    })?;

                let referenced = decode_janitor_object_ids(reference_rows)
                    .map_err(|error| janitor_pg_error("janitor current references are invalid", error))?;
                let snapshot_pins = decode_janitor_object_ids(pin_rows)
                    .map_err(|error| janitor_pg_error("janitor snapshot pins are invalid", error))?;
                let lease_rows = transaction.query(
                    "SELECT DISTINCT object_id FROM snapshot_payload_upload_objects WHERE vol = $1 AND object_id = ANY($2::BIGINT[]) AND (state IN ('uploading', 'uploaded', 'retry_wait') OR (reconcile_owner IS NOT NULL AND reconcile_lease_expires_at > clock_timestamp()))",
                    &[&volume_id, &object_ids],
                ).await.map_err(|error| janitor_pg_error("janitor upload lease query failed", error))?;
                let active_uploads = decode_janitor_object_ids(lease_rows).map_err(|error| janitor_pg_error("janitor upload lease IDs are invalid", error))?;
                let candidate_rows = transaction.query(
                    "SELECT object_id, floor(extract(epoch FROM eligible_at))::BIGINT, (extract(epoch FROM eligible_at) <= $3::BIGINT AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp())) FROM snapshot_payload_gc_candidates WHERE vol = $1 AND object_id = ANY($2::BIGINT[]) AND acknowledged_at IS NULL",
                    &[&volume_id, &object_ids, &eligible_before],
                ).await.map_err(|error| janitor_pg_error("janitor GC candidate state query failed", error))?;
                let gc_candidates = candidate_rows.into_iter().map(|row| {
                    let object_id = u64::try_from(row.get::<_, i64>(0)).map_err(|_| janitor_pg_error("janitor GC candidate IDs are invalid", "invalid object ID"))?;
                    Ok((object_id, (row.get::<_, i64>(1), row.get::<_, bool>(2))))
                }).collect::<Result<HashMap<_, _>, JanitorMetadataError>>()?;
                transaction.commit().await.map_err(|error| {
                    janitor_pg_error("janitor metadata read commit failed", error)
                })?;
                Ok(PgJanitorProtection {
                    referenced: referenced.union(&active_uploads).copied().collect(),
                    snapshot_pins,
                    gc_candidates,
                })
            })
            .await
        });
        match result {
            Ok(result) => result,
            Err(_) => Err(JanitorMetadataError::BackendUnavailable(
                "PostgreSQL janitor metadata query timed out".to_owned(),
            )),
        }
    }

    pub(crate) fn validate_protection(
        &mut self,
        local_info: &FormatInfo,
        local_version: u64,
    ) -> Result<(), JanitorMetadataError> {
        self.read_protection(local_info, local_version, &[], i64::MAX)
            .map(|_| ())
    }

    pub(crate) fn check_eligibility(
        &mut self,
        local_info: &FormatInfo,
        local_version: u64,
        object_ids: &[u64],
        eligible_before: i64,
    ) -> Result<PgJanitorProtection, JanitorMetadataError> {
        self.read_protection(local_info, local_version, object_ids, eligible_before)
    }

    pub(crate) fn gc_candidate_page(
        &mut self,
        after: Option<(i64, u64)>,
        requested_limit: usize,
        eligible_before: i64,
        backfill: bool,
    ) -> Result<Vec<(u64, i64)>, JanitorMetadataError> {
        let (Some(client), Some(runtime)) = (self.client.as_mut(), self.runtime.as_ref()) else {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "PostgreSQL janitor lock is not held",
            ));
        };
        let after = after
            .map(|(eligible_at, id)| {
                i64::try_from(id).map(|id| (eligible_at, id)).map_err(|_| {
                    JanitorMetadataError::UnsafeToDelete("janitor object id is invalid")
                })
            })
            .transpose()?;
        let limit =
            i64::try_from(requested_limit.min(PG_JANITOR_CANDIDATE_PAGE_SIZE)).map_err(|_| {
                JanitorMetadataError::UnsafeToDelete("janitor candidate page limit is invalid")
            })?;
        let statement = if after.is_some() {
            "SELECT object_id, floor(extract(epoch FROM eligible_at) * 1000000)::BIGINT FROM snapshot_payload_gc_candidates WHERE vol = $1 AND acknowledged_at IS NULL AND (eligible_at, object_id) > ('epoch'::timestamptz + $2::BIGINT * interval '1 microsecond', $3) AND extract(epoch FROM eligible_at) <= $4::BIGINT AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp()) ORDER BY eligible_at, object_id LIMIT $5"
        } else {
            "SELECT object_id, floor(extract(epoch FROM eligible_at) * 1000000)::BIGINT FROM snapshot_payload_gc_candidates WHERE vol = $1 AND acknowledged_at IS NULL AND extract(epoch FROM eligible_at) <= $2::BIGINT AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp()) ORDER BY eligible_at, object_id LIMIT $3"
        };
        let result = runtime.block_on(async {
            tokio::time::timeout(self.command_timeout, async {
                let tx = client.transaction().await?;
                // Dry runs only read the queue; ledger conversion and backfill are writes.
                if backfill {
                tx.execute(
                    "WITH legacy AS (SELECT object_id FROM snapshot_payload_upload_objects WHERE vol = $1 AND state = 'retry_wait' AND last_error IN ('remote deletion retry required', 'cli janitor remote deletion retry required') AND (reconcile_lease_expires_at IS NULL OR reconcile_lease_expires_at <= clock_timestamp()) ORDER BY object_id LIMIT $2 FOR UPDATE SKIP LOCKED) UPDATE snapshot_payload_upload_objects AS uploaded SET state = 'delete_pending', reconcile_owner = NULL, reconcile_lease_expires_at = NULL, next_retry_at = NULL WHERE uploaded.vol = $1 AND uploaded.object_id IN (SELECT object_id FROM legacy)",
                    &[&self.volume_id, &limit],
                )
                .await?;
                tx.execute(
                    "WITH pending AS (SELECT uploaded.object_id, COALESCE(uploaded.closed_at, uploaded.created_at) AS eligible_at, uploaded.next_retry_at FROM snapshot_payload_upload_objects AS uploaded LEFT JOIN snapshot_payload_gc_candidates AS candidate ON candidate.vol = uploaded.vol AND candidate.object_id = uploaded.object_id WHERE uploaded.vol = $1 AND uploaded.state = 'delete_pending' AND uploaded.reconcile_owner IS NULL AND candidate.object_id IS NULL AND extract(epoch FROM COALESCE(uploaded.closed_at, uploaded.created_at)) <= $2::BIGINT AND (uploaded.next_retry_at IS NULL OR uploaded.next_retry_at <= clock_timestamp()) ORDER BY eligible_at, uploaded.object_id LIMIT $3 FOR UPDATE OF uploaded SKIP LOCKED) INSERT INTO snapshot_payload_gc_candidates (vol, object_id, eligible_at, next_attempt_at) SELECT $1, object_id, eligible_at, next_retry_at FROM pending ON CONFLICT (vol, object_id) DO NOTHING",
                    &[&self.volume_id, &eligible_before, &limit],
                )
                .await?;
                }
                let rows = match after {
                    Some((eligible_at, object_id)) => tx.query(statement, &[&self.volume_id, &eligible_at, &object_id, &eligible_before, &limit]).await?,
                    None => tx.query(statement, &[&self.volume_id, &eligible_before, &limit]).await?,
                };
                tx.commit().await?;
                Ok::<_, tokio_postgres::Error>(rows)
            })
            .await
        });
        let rows = result
            .map_err(|_| {
                JanitorMetadataError::BackendUnavailable(
                    "PostgreSQL GC candidate page timed out".to_owned(),
                )
            })?
            .map_err(|_| {
                JanitorMetadataError::BackendUnavailable(
                    "PostgreSQL GC candidate page failed".to_owned(),
                )
            })?;
        rows.into_iter()
            .map(|row| {
                let object_id: i64 = row.get(0);
                let eligible_at: i64 = row.get(1);
                Ok((
                    u64::try_from(object_id).map_err(|_| {
                        JanitorMetadataError::UnsafeToDelete("janitor object id is invalid")
                    })?,
                    eligible_at,
                ))
            })
            .collect()
    }

    pub(crate) fn retry_gc_candidate(
        &mut self,
        object_id: u64,
    ) -> Result<(), JanitorMetadataError> {
        let (Some(client), Some(runtime)) = (self.client.as_ref(), self.runtime.as_ref()) else {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "PostgreSQL janitor lock is not held",
            ));
        };
        let object_id = i64::try_from(object_id)
            .map_err(|_| JanitorMetadataError::UnsafeToDelete("janitor object id is invalid"))?;
        let result = runtime.block_on(async {
            tokio::time::timeout(
                self.command_timeout,
                client.execute(
                    "UPDATE snapshot_payload_gc_candidates SET next_attempt_at = clock_timestamp() + interval '60 seconds', last_error = 'cli janitor deletion retry required' WHERE vol = $1 AND object_id = $2 AND acknowledged_at IS NULL",
                    &[&self.volume_id, &object_id],
                ),
            )
            .await
        });
        match result {
            Ok(Ok(1)) => Ok(()),
            Ok(Ok(_)) => Err(JanitorMetadataError::StaleClaim),
            Ok(Err(_)) => Err(JanitorMetadataError::BackendUnavailable(
                "PostgreSQL GC candidate retry update failed".to_owned(),
            )),
            Err(_) => Err(JanitorMetadataError::BackendUnavailable(
                "PostgreSQL GC candidate retry update timed out".to_owned(),
            )),
        }
    }

    pub(crate) fn ack_gc_candidate(&mut self, object_id: u64) -> Result<(), JanitorMetadataError> {
        let (Some(client), Some(runtime)) = (self.client.as_ref(), self.runtime.as_ref()) else {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "PostgreSQL janitor lock is not held",
            ));
        };
        let object_id = i64::try_from(object_id)
            .map_err(|_| JanitorMetadataError::UnsafeToDelete("janitor object id is invalid"))?;
        let result = runtime.block_on(async {
            tokio::time::timeout(
                self.command_timeout,
                client.execute(
                    "UPDATE snapshot_payload_gc_candidates AS candidate SET acknowledged_at = clock_timestamp(), next_attempt_at = NULL, last_error = NULL WHERE candidate.vol = $1 AND candidate.object_id = $2 AND candidate.acknowledged_at IS NULL AND EXISTS (SELECT 1 FROM snapshot_payload_upload_objects AS uploaded WHERE uploaded.vol = candidate.vol AND uploaded.object_id = candidate.object_id AND uploaded.state = 'delete_pending' AND uploaded.reconcile_owner IS NULL AND uploaded.last_error IS NULL)",
                    &[&self.volume_id, &object_id],
                ),
            )
            .await
        });
        match result {
            Ok(Ok(1)) => Ok(()),
            Ok(Ok(_)) => Err(JanitorMetadataError::StaleClaim),
            Ok(Err(_)) => Err(JanitorMetadataError::BackendUnavailable(
                "PostgreSQL GC candidate acknowledgement failed".to_owned(),
            )),
            Err(_) => Err(JanitorMetadataError::BackendUnavailable(
                "PostgreSQL GC candidate acknowledgement timed out".to_owned(),
            )),
        }
    }

    /// Claims one object for deletion under its object-level fence, immediately
    /// before remote I/O. Returns the fencing token, or `None` when protected.
    /// See [`claim_object_deletion_in`].
    pub(crate) fn claim_object_deletion(
        &mut self,
        object_id: u64,
        owner: &str,
    ) -> Result<Option<i64>, JanitorMetadataError> {
        let (Some(client), Some(runtime)) = (self.client.as_mut(), self.runtime.as_ref()) else {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "PostgreSQL janitor lock is not held",
            ));
        };
        let volume_id = self.volume_id;
        let object_id = i64::try_from(object_id)
            .map_err(|_| JanitorMetadataError::UnsafeToDelete("janitor object id is invalid"))?;
        let command_timeout = self.command_timeout;
        // The timer must be created inside the runtime, so build the timeout within block_on.
        let result = runtime.block_on(async move {
            tokio::time::timeout(command_timeout, async {
                let tx = client
                    .transaction()
                    .await
                    .map_err(|error| janitor_pg_error("janitor claim transaction failed", error))?;
                claim_object_deletion_in(tx, volume_id, object_id, owner)
                    .await
                    .map_err(|error| match error {
                        ObjectDeletionClaimError::VolumeBindingMissing => {
                            JanitorMetadataError::UnsafeToDelete(
                                "PostgreSQL volume binding is missing",
                            )
                        }
                        ObjectDeletionClaimError::Postgres(context, error) => {
                            janitor_pg_error(context, error)
                        }
                    })
            })
            .await
        });
        result.unwrap_or_else(|_| {
            Err(JanitorMetadataError::BackendUnavailable(
                "PostgreSQL janitor claim timed out".to_owned(),
            ))
        })
    }

    /// Completes a deletion claim; returns `false` when the claim is no longer
    /// current (expired and re-claimed, or otherwise fenced out).
    ///
    /// Both outcomes release the lease but keep `delete_pending`, the fence
    /// against a late publication. A retryable outcome records its reason in
    /// `last_error`, and the next cycle re-claims the row.
    pub(crate) fn finish_object_deletion(
        &mut self,
        object_id: u64,
        owner: &str,
        fencing_token: i64,
        outcome: crate::snapshot_ports::UploadedObjectDeletionOutcome,
    ) -> Result<bool, JanitorMetadataError> {
        use crate::snapshot_ports::UploadedObjectDeletionOutcome;
        let (Some(client), Some(runtime)) = (self.client.as_ref(), self.runtime.as_ref()) else {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "PostgreSQL janitor lock is not held",
            ));
        };
        let object_id = i64::try_from(object_id)
            .map_err(|_| JanitorMetadataError::UnsafeToDelete("janitor object id is invalid"))?;
        let message = match outcome {
            UploadedObjectDeletionOutcome::Deleted => None,
            UploadedObjectDeletionOutcome::Retryable => {
                Some("cli janitor remote deletion retry required")
            }
        };
        let result = runtime.block_on(async {
            tokio::time::timeout(
                self.command_timeout,
                client.execute(
                    super::snapshot_reachability::FINISH_UPLOADED_OBJECT_DELETION_SQL,
                    &[
                        &self.volume_id,
                        &object_id,
                        &owner,
                        &fencing_token,
                        &message,
                    ],
                ),
            )
            .await
        });
        match result {
            Ok(Ok(rows)) => Ok(rows == 1),
            Ok(Err(error)) => Err(janitor_pg_error("janitor claim finish failed", error)),
            Err(_) => Err(JanitorMetadataError::BackendUnavailable(
                "PostgreSQL janitor claim finish timed out".to_owned(),
            )),
        }
    }

    /// Deletes finished deletion fences below their payload namespace watermark,
    /// in bounded batches, while this session holds the Janitor lock. Returns the
    /// number of pruned rows. Registration and publication treat a row-less ID
    /// below the watermark as fenced, so pruning keeps the deleted ID fenced.
    pub(crate) fn prune_object_deletion_fences(&mut self) -> Result<u64, JanitorMetadataError> {
        const BATCH_ROWS: i64 = 1_024;
        const MAX_BATCHES: usize = 64;
        let (Some(client), Some(runtime)) = (self.client.as_ref(), self.runtime.as_ref()) else {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "PostgreSQL janitor lock is not held",
            ));
        };
        // A leased or failed fence (owner or last_error set) is still being deleted.
        let statement = format!(
            "DELETE FROM snapshot_payload_upload_objects WHERE vol = $1 AND object_id IN (SELECT object_id FROM snapshot_payload_upload_objects WHERE vol = $1 AND state IN ('delete_pending', 'deleted') AND reconcile_owner IS NULL AND last_error IS NULL AND {} ORDER BY object_id LIMIT $2 FOR UPDATE SKIP LOCKED)",
            super::snapshot_reachability::below_payload_watermark("object_id"),
        );
        let mut pruned = 0_u64;
        for _ in 0..MAX_BATCHES {
            let rows = runtime
                .block_on(async {
                    tokio::time::timeout(
                        self.command_timeout,
                        client.execute(&statement, &[&self.volume_id, &BATCH_ROWS]),
                    )
                    .await
                })
                .map_err(|_| {
                    JanitorMetadataError::BackendUnavailable(
                        "PostgreSQL janitor fence prune timed out".to_owned(),
                    )
                })?
                .map_err(|error| janitor_pg_error("janitor fence prune failed", error))?;
            pruned += rows;
            if rows < BATCH_ROWS as u64 {
                break;
            }
        }
        Ok(pruned)
    }
}

pub(super) enum ObjectDeletionClaimError {
    VolumeBindingMissing,
    Postgres(&'static str, tokio_postgres::Error),
}

/// Claims one object for deletion in `tx` under its object-level fence, shared
/// by the CLI and mounted Janitors. Returns the fencing token, or `None` when the
/// object is protected; only a claim is committed.
///
/// Event publication bumps `metadata_state` before it records references, so
/// the share lock taken first makes the reference and pin checks authoritative.
/// An untracked object receives a `delete_pending` ledger row in the same
/// transaction. Upload registration keeps that state and publication rejects
/// it, so a late upload or publication of the ID fails instead of reviving it.
pub(super) async fn claim_object_deletion_in(
    tx: tokio_postgres::Transaction<'_>,
    volume_id: i64,
    object_id: i64,
    owner: &str,
) -> Result<Option<i64>, ObjectDeletionClaimError> {
    use ObjectDeletionClaimError::Postgres;
    let lease_secs = super::snapshot_reachability::SNAPSHOT_UPLOADED_FRAGMENT_LEASE_SECS as f64;
    let pin_query = format!(
        "SELECT EXISTS ({SNAPSHOT_PIN_QUERY} AND reachability.object_id = $2) OR EXISTS (SELECT 1 FROM snapshot_current_references WHERE vol = $1 AND object_id = $2)"
    );
    tx.query_opt(
        "SELECT 1 FROM metadata_state WHERE vol = $1 FOR SHARE",
        &[&volume_id],
    )
    .await
    .map_err(|error| Postgres("janitor claim publication fence failed", error))?
    .ok_or(ObjectDeletionClaimError::VolumeBindingMissing)?;
    tx.execute(
        // The no-op update locks an existing row, so a concurrent fence prune skips it.
        "INSERT INTO snapshot_payload_upload_objects (vol, object_id, state, closed_at) VALUES ($1, $2, 'delete_pending', clock_timestamp()) ON CONFLICT (vol, object_id) DO UPDATE SET state = snapshot_payload_upload_objects.state",
        &[&volume_id, &object_id],
    )
    .await
    .map_err(|error| Postgres("janitor claim ledger insert failed", error))?;
    let lease_active: bool = tx
        .query_one(
            "SELECT reconcile_lease_expires_at IS NOT NULL AND reconcile_lease_expires_at > clock_timestamp() FROM snapshot_payload_upload_objects WHERE vol = $1 AND object_id = $2 FOR UPDATE",
            &[&volume_id, &object_id],
        )
        .await
        .map_err(|error| Postgres("janitor claim ledger read failed", error))?
        .get(0);
    let protected = lease_active
        || tx
            .query_one(&pin_query, &[&volume_id, &object_id])
            .await
            .map_err(|error| Postgres("janitor claim protection check failed", error))?
            .get::<_, bool>(0);
    let fencing_token = if protected {
        None
    } else {
        // Same protection as eligibility: an upload awaiting publication
        // ('uploading', 'uploaded', 'retry_wait') is never claimed.
        tx.query_opt(
            "UPDATE snapshot_payload_upload_objects SET state = 'delete_pending', reconcile_owner = $3, reconcile_lease_expires_at = clock_timestamp() + ($4 * interval '1 second'), reconcile_fence = reconcile_fence + 1, next_retry_at = NULL, last_error = NULL WHERE vol = $1 AND object_id = $2 AND state IN ('published', 'delete_pending', 'deleted') RETURNING reconcile_fence",
            &[&volume_id, &object_id, &owner, &lease_secs],
        )
        .await
        .map_err(|error| Postgres("janitor claim lease failed", error))?
        .map(|row| row.get::<_, i64>(0))
    };
    // Commit only a claim, so a protected object keeps its prior ledger state.
    if fencing_token.is_some() {
        tx.execute(
            "INSERT INTO snapshot_payload_gc_candidates (vol, object_id) VALUES ($1, $2) ON CONFLICT (vol, object_id) DO NOTHING",
            &[&volume_id, &object_id],
        )
        .await
        .map_err(|error| Postgres("janitor GC candidate enqueue failed", error))?;
        tx.commit()
            .await
            .map_err(|error| Postgres("janitor claim commit failed", error))?;
    } else {
        tx.rollback()
            .await
            .map_err(|error| Postgres("janitor claim rollback failed", error))?;
    }
    Ok(fencing_token)
}

/// Drops the source error so PostgreSQL details (possibly the DSN) never reach CLI output.
fn janitor_pg_error(context: &'static str, _error: impl std::fmt::Display) -> JanitorMetadataError {
    JanitorMetadataError::BackendUnavailable(context.to_owned())
}

impl Drop for PgJanitorSession {
    fn drop(&mut self) {
        self.close_connection();
    }
}

fn decode_janitor_object_ids(rows: Vec<tokio_postgres::Row>) -> MetaResult<HashSet<u64>> {
    rows.into_iter()
        .map(|row| {
            u64::try_from(row.get::<_, i64>(0))
                .map_err(|_| MetaError::Storage("janitor object id is invalid".to_owned()))
        })
        .collect()
}

pub(super) struct PgJanitorLock {
    pub(super) _runtime: tokio::runtime::Runtime,
    pub(super) _client: tokio_postgres::Client,
    pub(super) connection_task: tokio::task::JoinHandle<()>,
}

impl PgMetaEngine {
    fn janitor_lock_key(&self) -> i64 {
        pg_janitor_lock_key(self.volume_id)
    }

    pub(super) fn try_acquire_pg_janitor_lock(&self) -> MetaResult<bool> {
        let mut state = self
            .janitor_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg janitor lock state poisoned".to_string()))?;
        if state.is_some() {
            return Ok(false);
        }

        let lock_key = self.janitor_lock_key();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|e| MetaError::Storage(format!("failed to build tokio runtime: {e}")))?;
        let connect = connect_studiofs_postgres(&self.postgres_url);
        let (client, connection) = runtime
            .block_on(async { tokio::time::timeout(self.connect_timeout, connect).await })
            .map_err(|_| {
                Self::pg_timeout_error(
                    PgTimeoutContext::Connection("janitor lock connect"),
                    self.connect_timeout,
                )
            })??;

        let connection_task = runtime.spawn(async move {
            let _ = connection.await;
        });
        let acquired = runtime
            .block_on(async {
                tokio::time::timeout(
                    self.command_timeout,
                    client.query_one("SELECT pg_try_advisory_lock($1)", &[&lock_key]),
                )
                .await
            })
            .map_err(|_| {
                Self::pg_timeout_error(
                    PgTimeoutContext::Command("janitor lock acquire"),
                    self.command_timeout,
                )
            })?
            .map_err(|e| Self::classify_pg_error("postgres janitor lock acquire failed", e))?
            .get::<usize, bool>(0);

        if !acquired {
            connection_task.abort();
            return Ok(false);
        }

        *state = Some(PgJanitorLock {
            _runtime: runtime,
            _client: client,
            connection_task,
        });
        Ok(true)
    }

    pub(super) fn release_pg_janitor_lock(&self) -> MetaResult<()> {
        let mut state = self
            .janitor_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg janitor lock state poisoned".to_string()))?;
        let Some(lock) = state.take() else {
            return Ok(());
        };
        lock.connection_task.abort();
        Ok(())
    }

    pub(super) fn load_pg_global_snapshot_pins(&self) -> MetaResult<HashSet<u64>> {
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;
        let rows = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "janitor snapshot pin query",
                move |client| {
                    Box::pin(async move {
                        let bootstrap_ready = client
                            .query_opt(
                                "SELECT initialized_version FROM snapshot_reachability_bootstrap WHERE vol = $1",
                                &[&volume_id],
                            )
                            .await
                            .map_err(|e| {
                                Self::classify_pg_error(
                                    "postgres janitor snapshot bootstrap query failed",
                                    e,
                                )
                            })?;
                        if bootstrap_ready.is_none() {
                            return Err(MetaError::Storage(
                                "snapshot reachability bootstrap incomplete".to_owned(),
                            ));
                        }
                        client
                            .query(
                                SNAPSHOT_PIN_QUERY,
                                &[&volume_id],
                            )
                            .await
                            .map_err(|e| {
                                Self::classify_pg_error(
                                    "postgres janitor snapshot pin query failed",
                                    e,
                                )
                            })
                    })
                },
            )
            .await
        })?;

        let mut pins = HashSet::new();
        for row in rows {
            let object_id: i64 = row.get(0);
            let object_id = u64::try_from(object_id).map_err(|_| {
                MetaError::Storage(format!(
                    "snapshot artifact object id must be non-negative: {object_id}"
                ))
            })?;
            pins.insert(object_id);
        }
        Ok(pins)
    }
}

pub(crate) fn pg_janitor_lock_key(volume_id: i64) -> i64 {
    volume_id.wrapping_mul(1_000_003) ^ PG_JANITOR_LOCK_SALT
}

const PG_EVENT_LOG_PRUNE_LOCK_SALT: i64 = 0x5f53_4653_5f50_5255;

pub(super) fn pg_event_log_prune_lock_key(volume_id: i64) -> i64 {
    volume_id.wrapping_mul(1_000_003) ^ PG_EVENT_LOG_PRUNE_LOCK_SALT
}

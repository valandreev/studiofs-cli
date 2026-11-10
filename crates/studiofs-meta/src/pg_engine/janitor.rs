use super::{PgMetaEngine, PgTimeoutContext};
use crate::engine::{MetaError, MetaResult};
use crate::janitor::{
    JanitorDeletionSettlement, JanitorMetadataError, JanitorSweepCheckpoint, JanitorSweepScope,
};
use crate::pg_transport::connect_studiofs_postgres;
use crate::schema::FormatInfo;
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, Statement};

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

/// [`SNAPSHOT_PIN_QUERY`] restricted to the object IDs in `$2`.
static BATCH_PIN_QUERY: LazyLock<String> = LazyLock::new(|| {
    format!("{SNAPSHOT_PIN_QUERY} AND reachability.object_id = ANY($2::BIGINT[])")
});

pub(crate) struct PgJanitorSession {
    postgres_url: String,
    volume_id: i64,
    connect_timeout: Duration,
    command_timeout: Duration,
    runtime: Option<Runtime>,
    client: Option<Client>,
    connection_task: Option<JoinHandle<()>>,
    /// Statements prepared on `client`, keyed by SQL; cleared with the connection.
    statements: HashMap<String, Statement>,
}

/// Prepares the statements this connection has not prepared yet, all in one
/// pipelined round trip. A prepared statement then costs one round trip per use,
/// and statements sent together without awaiting share a single round trip.
async fn prepare_statements<const N: usize>(
    client: &Client,
    cache: &mut HashMap<String, Statement>,
    sql: [&str; N],
) -> Result<[Statement; N], JanitorMetadataError> {
    let missing: Vec<&str> = sql
        .iter()
        .copied()
        .filter(|sql| !cache.contains_key(*sql))
        .collect();
    let prepared = futures::future::try_join_all(missing.iter().map(|sql| client.prepare(sql)))
        .await
        .map_err(|error| janitor_pg_error("janitor statement prepare failed", error))?;
    cache.extend(missing.into_iter().map(str::to_owned).zip(prepared));
    Ok(sql.map(|sql| cache[sql].clone()))
}

pub(crate) struct PgJanitorProtection {
    pub(crate) referenced: HashSet<u64>,
    pub(crate) snapshot_pins: HashSet<u64>,
    pub(crate) gc_candidates: HashMap<u64, (i64, bool)>,
}

pub(crate) const PG_JANITOR_CANDIDATE_PAGE_SIZE: usize = crate::MAX_JANITOR_ELIGIBILITY_BATCH;

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

/// PostgreSQL is the reference authority, so a local replica behind it is safe;
/// one ahead means PostgreSQL lost history (e.g. a restored backup).
fn local_is_ahead(local_version: u64, remote_head: i64) -> bool {
    i64::try_from(local_version).map_or(true, |local| local > remote_head)
}

fn pg_sweep_timeout(timeout: Duration) -> JanitorMetadataError {
    JanitorMetadataError::BackendUnavailable(format!(
        "PostgreSQL janitor sweep checkpoint timed out after {} seconds",
        timeout.as_secs()
    ))
}

/// Rejects a scan position that is not a safe key under the scope's prefix or
/// whose pending object does not match its pending key.
fn validate_sweep_checkpoint(
    scope: &JanitorSweepScope<'_>,
    checkpoint: &JanitorSweepCheckpoint,
) -> Result<(), JanitorMetadataError> {
    let prefix = scope.data_prefix;
    let after_key = checkpoint.after_key.as_deref();
    let pending_key = checkpoint.pending_key.as_deref();
    let valid = checkpoint.pending_object_id.is_some() == pending_key.is_some()
        && (pending_key.is_none() || after_key.is_some())
        && after_key.is_none_or(|key| {
            safe_sweep_key(prefix, key) && pending_key.is_none_or(|pending| key >= pending)
        })
        && pending_key.is_none_or(|key| {
            safe_sweep_key(prefix, key)
                && canonical_sweep_object_id(prefix, key) == checkpoint.pending_object_id
        });
    if valid {
        Ok(())
    } else {
        Err(JanitorMetadataError::UnsafeToDelete(
            "janitor sweep checkpoint position is invalid",
        ))
    }
}

async fn persist_sweep_checkpoint(
    client: &Client,
    statements: &mut HashMap<String, Statement>,
    volume_id: i64,
    scope: &JanitorSweepScope<'_>,
    checkpoint: &JanitorSweepCheckpoint,
) -> Result<(), JanitorMetadataError> {
    validate_sweep_checkpoint(scope, checkpoint)?;
    let pending_object_id = checkpoint
        .pending_object_id
        .map(i64::try_from)
        .transpose()
        .map_err(|_| {
            JanitorMetadataError::UnsafeToDelete("janitor sweep checkpoint object id is invalid")
        })?;
    let [statement] = prepare_statements(client, statements, [
            "INSERT INTO snapshot_payload_sweep_checkpoints (vol, schema_version, volume_uuid, endpoint, bucket, data_prefix, listing_strategy, after_key, pending_object_id, pending_key) VALUES ($1, 1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (vol) DO UPDATE SET schema_version = EXCLUDED.schema_version, volume_uuid = EXCLUDED.volume_uuid, endpoint = EXCLUDED.endpoint, bucket = EXCLUDED.bucket, data_prefix = EXCLUDED.data_prefix, listing_strategy = EXCLUDED.listing_strategy, after_key = EXCLUDED.after_key, pending_object_id = EXCLUDED.pending_object_id, pending_key = EXCLUDED.pending_key WHERE snapshot_payload_sweep_checkpoints.schema_version = 1",
    ])
    .await?;
    let affected = client
        .execute(
            &statement,
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
            statements: HashMap::new(),
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
        let statements = &mut self.statements;
        let volume_id = self.volume_id;
        let timeout = self.command_timeout;
        let result = runtime.block_on(async {
            tokio::time::timeout(timeout, async {
                let row = client
                    .query_opt(
                        "SELECT schema_version, volume_uuid, endpoint, bucket, data_prefix, listing_strategy, after_key, pending_object_id, pending_key FROM snapshot_payload_sweep_checkpoints WHERE vol = $1",
                        &[&volume_id],
                    )
                    .await
                    .map_err(|error| {
                        janitor_pg_error("janitor sweep checkpoint read failed", error)
                    })?;
                if let Some(row) = row {
                    if row.get::<_, i16>(0) != 1 {
                        return Err(JanitorMetadataError::UnsafeToDelete(
                            "janitor sweep checkpoint schema is unsupported",
                        ));
                    }
                    let stored_scope: [String; 5] =
                        [row.get(1), row.get(2), row.get(3), row.get(4), row.get(5)];
                    if stored_scope
                        == [
                            scope.volume_uuid,
                            scope.endpoint,
                            scope.bucket,
                            scope.data_prefix,
                            scope.listing_strategy,
                        ]
                    {
                        let pending_object_id = row
                            .get::<_, Option<i64>>(7)
                            .map(u64::try_from)
                            .transpose()
                            .map_err(|_| {
                                JanitorMetadataError::UnsafeToDelete(
                                    "janitor sweep checkpoint object id is invalid",
                                )
                            })?;
                        let checkpoint = JanitorSweepCheckpoint {
                            after_key: row.get(6),
                            pending_object_id,
                            pending_key: row.get(8),
                        };
                        validate_sweep_checkpoint(&scope, &checkpoint)?;
                        return Ok(checkpoint);
                    }
                }
                // No row, or one saved for another storage scope: the sweep starts over.
                let checkpoint = JanitorSweepCheckpoint::default();
                if persist_initial {
                    persist_sweep_checkpoint(client, statements, volume_id, &scope, &checkpoint)
                        .await?;
                }
                Ok(checkpoint)
            })
            .await
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
        let statements = &mut self.statements;
        let timeout = self.command_timeout;
        let volume_id = self.volume_id;
        runtime
            .block_on(async {
                tokio::time::timeout(
                    timeout,
                    persist_sweep_checkpoint(client, statements, volume_id, &scope, checkpoint),
                )
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
        self.statements.clear();
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
        let statements = &mut self.statements;
        let client = &*client;
        let result = runtime.block_on(async {
            tokio::time::timeout(command_timeout, async {
                let [identity, bootstrap, references, pins, leases, candidates] =
                    prepare_statements(client, statements, [
                        "SELECT volumes.uuid, metadata_state.version, metadata_state.metadata_format_version, metadata_state.format_info, metadata_state.checkpoint_publication_state, metadata_state.checkpoint_generation FROM volumes INNER JOIN metadata_state ON metadata_state.vol = volumes.vol WHERE volumes.vol = $1",
                        "SELECT initialized_version FROM snapshot_reachability_bootstrap WHERE vol = $1",
                        "SELECT DISTINCT object_id FROM snapshot_current_references WHERE vol = $1 AND object_id = ANY($2::BIGINT[])",
                        &BATCH_PIN_QUERY,
                        "SELECT DISTINCT object_id FROM snapshot_payload_upload_objects WHERE vol = $1 AND object_id = ANY($2::BIGINT[]) AND (state IN ('uploading', 'uploaded', 'retry_wait') OR (reconcile_owner IS NOT NULL AND reconcile_lease_expires_at > clock_timestamp()))",
                        "SELECT object_id, floor(extract(epoch FROM eligible_at))::BIGINT, (extract(epoch FROM eligible_at) <= $3::BIGINT AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp())) FROM snapshot_payload_gc_candidates WHERE vol = $1 AND object_id = ANY($2::BIGINT[]) AND acknowledged_at IS NULL",
                    ])
                    .await?;
                // One pipelined round trip: PostgreSQL still runs the statements in
                // order inside one read-only snapshot. A failed statement aborts the
                // transaction, so the trailing COMMIT only rolls it back.
                let (begin, row, bootstrap, reference_rows, pin_rows, lease_rows, candidate_rows, commit) = tokio::join!(
                    // Each request is sent on its first poll; `biased` keeps that in order.
                    biased;
                    client.batch_execute("START TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"),
                    async { client.query_opt(&identity, &[&volume_id]).await },
                    async { client.query_opt(&bootstrap, &[&volume_id]).await },
                    async { client.query(&references, &[&volume_id, &object_ids]).await },
                    async { client.query(&pins, &[&volume_id, &object_ids]).await },
                    async { client.query(&leases, &[&volume_id, &object_ids]).await },
                    async { client.query(&candidates, &[&volume_id, &object_ids, &eligible_before]).await },
                    client.batch_execute("COMMIT"),
                );
                begin.map_err(|error| {
                    janitor_pg_error("janitor read-only transaction failed", error)
                })?;
                let row = row
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
                if local_is_ahead(local_version, remote_head) {
                    return Err(JanitorMetadataError::UnsafeToDelete(
                        "local metadata is ahead of PostgreSQL",
                    ));
                }

                let bootstrap = bootstrap.map_err(|error| {
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
                let reference_rows = reference_rows.map_err(|error| {
                    janitor_pg_error("janitor current-reference query failed", error)
                })?;
                let pin_rows = pin_rows
                    .map_err(|error| janitor_pg_error("janitor snapshot pin query failed", error))?;

                let invalid_ids = |context: &'static str| {
                    move |_| JanitorMetadataError::BackendUnavailable(context.to_owned())
                };
                let referenced = decode_janitor_object_ids(reference_rows)
                    .map_err(invalid_ids("janitor current references are invalid"))?;
                let snapshot_pins = decode_janitor_object_ids(pin_rows)
                    .map_err(invalid_ids("janitor snapshot pins are invalid"))?;
                let lease_rows = lease_rows.map_err(|error| janitor_pg_error("janitor upload lease query failed", error))?;
                let active_uploads = decode_janitor_object_ids(lease_rows).map_err(invalid_ids("janitor upload lease IDs are invalid"))?;
                let candidate_rows = candidate_rows.map_err(|error| janitor_pg_error("janitor GC candidate state query failed", error))?;
                let gc_candidates = candidate_rows.into_iter().map(|row| {
                    let object_id = u64::try_from(row.get::<_, i64>(0)).map_err(|_| JanitorMetadataError::BackendUnavailable("janitor GC candidate IDs are invalid".to_owned()))?;
                    Ok((object_id, (row.get::<_, i64>(1), row.get::<_, bool>(2))))
                }).collect::<Result<HashMap<_, _>, JanitorMetadataError>>()?;
                commit.map_err(|error| {
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
        /// Legacy ledger rows enqueued by the first page of one cycle.
        const BACKFILL_ROWS: i64 = 4_096;
        // Dry runs only read the queue; ledger conversion and backfill are writes.
        // Later pages of a cycle only read: the first page already enqueued the backlog.
        let backfill = backfill && after.is_none();
        let statements = &mut self.statements;
        let client = &*client;
        let volume_id = self.volume_id;
        let result = runtime.block_on(async {
            tokio::time::timeout(self.command_timeout, async {
                let [legacy, pending, first_page, next_page] = prepare_statements(client, statements, [
                    "WITH legacy AS (SELECT object_id FROM snapshot_payload_upload_objects WHERE vol = $1 AND state = 'retry_wait' AND last_error IN ('remote deletion retry required', 'cli janitor remote deletion retry required') AND (reconcile_lease_expires_at IS NULL OR reconcile_lease_expires_at <= clock_timestamp()) ORDER BY object_id LIMIT $2 FOR UPDATE SKIP LOCKED) UPDATE snapshot_payload_upload_objects AS uploaded SET state = 'delete_pending', reconcile_owner = NULL, reconcile_lease_expires_at = NULL, next_retry_at = NULL WHERE uploaded.vol = $1 AND uploaded.object_id IN (SELECT object_id FROM legacy)",
                    "WITH pending AS (SELECT uploaded.object_id, COALESCE(uploaded.closed_at, uploaded.created_at) AS eligible_at, uploaded.next_retry_at FROM snapshot_payload_upload_objects AS uploaded LEFT JOIN snapshot_payload_gc_candidates AS candidate ON candidate.vol = uploaded.vol AND candidate.object_id = uploaded.object_id WHERE uploaded.vol = $1 AND uploaded.state = 'delete_pending' AND uploaded.reconcile_owner IS NULL AND candidate.object_id IS NULL AND extract(epoch FROM COALESCE(uploaded.closed_at, uploaded.created_at)) <= $2::BIGINT AND (uploaded.next_retry_at IS NULL OR uploaded.next_retry_at <= clock_timestamp()) ORDER BY eligible_at, uploaded.object_id LIMIT $3 FOR UPDATE OF uploaded SKIP LOCKED) INSERT INTO snapshot_payload_gc_candidates (vol, object_id, eligible_at, next_attempt_at) SELECT $1, object_id, eligible_at, next_retry_at FROM pending ON CONFLICT (vol, object_id) DO NOTHING",
                    "SELECT object_id, floor(extract(epoch FROM eligible_at) * 1000000)::BIGINT FROM snapshot_payload_gc_candidates WHERE vol = $1 AND acknowledged_at IS NULL AND extract(epoch FROM eligible_at) <= $2::BIGINT AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp()) ORDER BY eligible_at, object_id LIMIT $3",
                    "SELECT object_id, floor(extract(epoch FROM eligible_at) * 1000000)::BIGINT FROM snapshot_payload_gc_candidates WHERE vol = $1 AND acknowledged_at IS NULL AND (eligible_at, object_id) > ('epoch'::timestamptz + $2::BIGINT * interval '1 microsecond', $3) AND extract(epoch FROM eligible_at) <= $4::BIGINT AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp()) ORDER BY eligible_at, object_id LIMIT $5",
                ])
                .await?;
                // One pipelined round trip; a failed statement turns the COMMIT into a rollback.
                let (begin, legacy, pending, rows, commit) = tokio::join!(
                    biased;
                    client.batch_execute("BEGIN"),
                    async {
                        if backfill {
                            client.execute(&legacy, &[&volume_id, &BACKFILL_ROWS]).await?;
                        }
                        Ok(())
                    },
                    async {
                        if backfill {
                            client
                                .execute(&pending, &[&volume_id, &eligible_before, &BACKFILL_ROWS])
                                .await?;
                        }
                        Ok::<_, tokio_postgres::Error>(())
                    },
                    async {
                        match after {
                            Some((eligible_at, object_id)) => {
                                client
                                    .query(
                                        &next_page,
                                        &[&volume_id, &eligible_at, &object_id, &eligible_before, &limit],
                                    )
                                    .await
                            }
                            None => {
                                client
                                    .query(&first_page, &[&volume_id, &eligible_before, &limit])
                                    .await
                            }
                        }
                    },
                    client.batch_execute("COMMIT"),
                );
                let page_error =
                    |error| janitor_pg_error("PostgreSQL GC candidate page failed", error);
                begin.map_err(page_error)?;
                legacy.map_err(page_error)?;
                pending.map_err(page_error)?;
                let rows = rows.map_err(page_error)?;
                commit.map_err(page_error)?;
                Ok(rows)
            })
            .await
        });
        let rows = result.map_err(|_| {
            JanitorMetadataError::BackendUnavailable(
                "PostgreSQL GC candidate page timed out".to_owned(),
            )
        })??;
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

    /// Claims the given objects for deletion under their object-level fences,
    /// immediately before remote I/O, in two round trips. Returns the fencing token
    /// of each claimed object; a missing object is protected. See
    /// [`CLAIM_OBJECT_DELETIONS_SQL`].
    pub(crate) fn claim_object_deletions(
        &mut self,
        object_ids: &[u64],
        owner: &str,
    ) -> Result<HashMap<u64, i64>, JanitorMetadataError> {
        let (Some(client), Some(runtime)) = (self.client.as_ref(), self.runtime.as_ref()) else {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "PostgreSQL janitor lock is not held",
            ));
        };
        let object_ids = janitor_object_ids(object_ids)?;
        let statements = &mut self.statements;
        let volume_id = self.volume_id;
        let lease_secs = super::snapshot_reachability::SNAPSHOT_UPLOADED_FRAGMENT_LEASE_SECS as f64;
        let result = runtime.block_on(async {
            tokio::time::timeout(self.command_timeout, async {
                let [fence, claim] = prepare_statements(
                    client,
                    statements,
                    [CLAIM_PUBLICATION_FENCE_SQL, &CLAIM_OBJECT_DELETIONS_SQL],
                )
                .await?;
                // Two round trips. The claim is sent only after the share lock is granted:
                // a claim pipelined behind a waiting lock would still commit, and keep its
                // lease, after this process died.
                let (begin, fence) = tokio::join!(
                    // Each request is sent on its first poll; `biased` keeps that in order.
                    biased;
                    client.batch_execute("BEGIN"),
                    async { client.query_opt(&fence, &[&volume_id]).await },
                );
                let fenced = begin
                    .map_err(|error| janitor_pg_error("janitor claim transaction failed", error))
                    .and_then(|()| {
                        fence.map_err(|error| {
                            janitor_pg_error("janitor claim publication fence failed", error)
                        })
                    })
                    .and_then(|row| {
                        row.map(drop).ok_or(JanitorMetadataError::UnsafeToDelete(
                            "PostgreSQL volume binding is missing",
                        ))
                    });
                if let Err(error) = fenced {
                    // Leave the connection outside a transaction; the fence error wins.
                    let _ = client.batch_execute("ROLLBACK").await;
                    return Err(error);
                }
                // A failed claim aborts the transaction, so this COMMIT only rolls it back.
                let (claimed, commit) = tokio::join!(
                    biased;
                    async {
                        client
                            .query(&claim, &[&volume_id, &object_ids, &owner, &lease_secs])
                            .await
                    },
                    client.batch_execute("COMMIT"),
                );
                let claimed = claimed
                    .map_err(|error| janitor_pg_error("janitor claim lease failed", error))?;
                commit.map_err(|error| janitor_pg_error("janitor claim commit failed", error))?;
                claimed
                    .into_iter()
                    .map(|row| {
                        let object_id = u64::try_from(row.get::<_, i64>(0)).map_err(|_| {
                            JanitorMetadataError::BackendUnavailable(
                                "janitor claimed object id is invalid".to_owned(),
                            )
                        })?;
                        Ok((object_id, row.get::<_, i64>(1)))
                    })
                    .collect()
            })
            .await
        });
        result.unwrap_or_else(|_| {
            Err(JanitorMetadataError::BackendUnavailable(
                "PostgreSQL janitor claim timed out".to_owned(),
            ))
        })
    }

    /// Settles a batch in one statement; see [`JanitorDeletionSettlement`]. Returns
    /// the objects whose claim was still current; any other claim was fenced out.
    ///
    /// A settled claim releases the lease but keeps `delete_pending`, the fence
    /// against a late publication. A claim without a confirmed deletion records its
    /// reason in `last_error`, and a later cycle re-claims the row.
    pub(crate) fn settle_object_deletions(
        &mut self,
        settlements: &[JanitorDeletionSettlement],
        owner: &str,
    ) -> Result<HashSet<u64>, JanitorMetadataError> {
        use crate::snapshot_ports::UploadedObjectDeletionOutcome::Deleted;
        use JanitorDeletionSettlement::{Finished, Released};
        let (Some(client), Some(runtime)) = (self.client.as_ref(), self.runtime.as_ref()) else {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "PostgreSQL janitor lock is not held",
            ));
        };
        let object_ids =
            janitor_object_ids(settlements.iter().map(JanitorDeletionSettlement::object_id))?;
        let fences: Vec<Option<i64>> = settlements
            .iter()
            .map(JanitorDeletionSettlement::fencing_token)
            .collect();
        let errors: Vec<Option<&str>> = settlements
            .iter()
            .map(|settlement| match settlement {
                Finished {
                    outcome: Deleted, ..
                } => None,
                _ => Some("cli janitor remote deletion retry required"),
            })
            .collect();
        let defers: Vec<bool> = settlements
            .iter()
            .map(|settlement| !matches!(settlement, Released { .. }))
            .collect();
        let statements = &mut self.statements;
        let volume_id = self.volume_id;
        let result = runtime.block_on(async {
            tokio::time::timeout(self.command_timeout, async {
                let [settle] =
                    prepare_statements(client, statements, [SETTLE_OBJECT_DELETIONS_SQL]).await?;
                let rows = client
                    .query(
                        &settle,
                        &[&volume_id, &object_ids, &owner, &fences, &errors, &defers],
                    )
                    .await
                    .map_err(|error| janitor_pg_error("janitor claim finish failed", error))?;
                decode_janitor_object_ids(rows).map_err(|_| {
                    JanitorMetadataError::BackendUnavailable(
                        "janitor finished object id is invalid".to_owned(),
                    )
                })
            })
            .await
        });
        result.unwrap_or_else(|_| {
            Err(JanitorMetadataError::BackendUnavailable(
                "PostgreSQL janitor claim finish timed out".to_owned(),
            ))
        })
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
        let fence_prune = format!(
            "DELETE FROM snapshot_payload_upload_objects WHERE vol = $1 AND object_id IN (SELECT object_id FROM snapshot_payload_upload_objects WHERE vol = $1 AND state IN ('delete_pending', 'deleted') AND reconcile_owner IS NULL AND last_error IS NULL AND {} ORDER BY object_id LIMIT $2 FOR UPDATE SKIP LOCKED)",
            super::snapshot_reachability::below_payload_watermark("object_id"),
        );
        let statements = &mut self.statements;
        let volume_id = self.volume_id;
        let timeout = self.command_timeout;
        let result = runtime.block_on(async {
            tokio::time::timeout(timeout, async {
                let [fence_prune, queue_prune] = prepare_statements(client, statements, [
                    &fence_prune,
                    // An acknowledged queue row only stops the backfill from re-enqueueing its
                    // fence; once the fence row is pruned it has no purpose left.
                    "DELETE FROM snapshot_payload_gc_candidates WHERE vol = $1 AND object_id IN (SELECT candidate.object_id FROM snapshot_payload_gc_candidates AS candidate WHERE candidate.vol = $1 AND candidate.acknowledged_at IS NOT NULL AND NOT EXISTS (SELECT 1 FROM snapshot_payload_upload_objects AS uploaded WHERE uploaded.vol = candidate.vol AND uploaded.object_id = candidate.object_id) LIMIT $2)",
                ])
                .await?;
                let mut pruned = 0_u64;
                for _ in 0..MAX_BATCHES {
                    let rows = client
                        .execute(&fence_prune, &[&volume_id, &BATCH_ROWS])
                        .await
                        .map_err(|error| janitor_pg_error("janitor fence prune failed", error))?;
                    pruned += rows;
                    if rows < BATCH_ROWS as u64 {
                        break;
                    }
                }
                client
                    .execute(&queue_prune, &[&volume_id, &(BATCH_ROWS * MAX_BATCHES as i64)])
                    .await
                    .map_err(|error| janitor_pg_error("janitor queue prune failed", error))?;
                Ok(pruned)
            })
            .await
        });
        result.unwrap_or_else(|_| {
            Err(JanitorMetadataError::BackendUnavailable(
                "PostgreSQL janitor fence prune timed out".to_owned(),
            ))
        })
    }
}

fn janitor_object_ids<'a>(
    object_ids: impl IntoIterator<Item = &'a u64>,
) -> Result<Vec<i64>, JanitorMetadataError> {
    object_ids
        .into_iter()
        .map(|id| {
            i64::try_from(*id)
                .map_err(|_| JanitorMetadataError::UnsafeToDelete("janitor object id is invalid"))
        })
        .collect()
}

/// Publication bumps `metadata_state` before it records references, so this share
/// lock, taken before the claim statement starts, makes the claim's reference and pin
/// checks authoritative.
pub(super) const CLAIM_PUBLICATION_FENCE_SQL: &str =
    "SELECT 1 FROM metadata_state WHERE vol = $1 FOR SHARE";

/// Claims every object in `$2` that is neither referenced nor snapshot-pinned, under
/// its object-level fence, and returns `(object_id, fencing_token)` per claim
/// (`$3` owner, `$4` lease seconds). Shared by the CLI and mounted Janitors; run it
/// after [`CLAIM_PUBLICATION_FENCE_SQL`] in the same transaction.
///
/// A protected object keeps its prior ledger state. An untracked object receives a
/// `delete_pending` ledger row. Upload registration keeps that state and publication
/// rejects it, so a late upload or publication of the ID fails instead of reviving
/// it. An existing row is claimed only from `published`, `delete_pending` or
/// `deleted` with no active lease: an upload awaiting publication ('uploading',
/// 'uploaded', 'retry_wait') is never claimed, as in eligibility. Rows are locked in
/// object ID order.
pub(super) static CLAIM_OBJECT_DELETIONS_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "WITH requested AS (SELECT DISTINCT object_id FROM unnest($2::BIGINT[]) AS requested(object_id)), protected AS (SELECT object_id FROM snapshot_current_references WHERE vol = $1 AND object_id = ANY($2::BIGINT[]) UNION {pins}), claimed AS (INSERT INTO snapshot_payload_upload_objects AS uploaded (vol, object_id, state, closed_at, reconcile_owner, reconcile_lease_expires_at, reconcile_fence) SELECT $1, requested.object_id, 'delete_pending', clock_timestamp(), $3::TEXT, clock_timestamp() + ($4::FLOAT8 * interval '1 second'), 1 FROM requested WHERE requested.object_id NOT IN (SELECT object_id FROM protected) AND EXISTS (SELECT 1 FROM metadata_state WHERE vol = $1) ORDER BY requested.object_id ON CONFLICT (vol, object_id) DO UPDATE SET state = 'delete_pending', reconcile_owner = EXCLUDED.reconcile_owner, reconcile_lease_expires_at = EXCLUDED.reconcile_lease_expires_at, reconcile_fence = uploaded.reconcile_fence + 1, next_retry_at = NULL, last_error = NULL WHERE uploaded.state IN ('published', 'delete_pending', 'deleted') AND (uploaded.reconcile_lease_expires_at IS NULL OR uploaded.reconcile_lease_expires_at <= clock_timestamp()) RETURNING uploaded.object_id, uploaded.reconcile_fence), enqueued AS (INSERT INTO snapshot_payload_gc_candidates (vol, object_id) SELECT $1, object_id FROM claimed ON CONFLICT (vol, object_id) DO NOTHING) SELECT object_id, reconcile_fence FROM claimed",
        pins = *BATCH_PIN_QUERY,
    )
});

/// Settles a batch (`$2` object IDs, `$4` fencing tokens, `$5` retry reasons, `$6`
/// defer flags; `$3` owner): finishes each current claim as
/// [`super::snapshot_reachability::FINISH_UPLOADED_OBJECT_DELETION_SQL`] does,
/// acknowledges the queue rows of confirmed deletions, and defers every other
/// flagged queue row by 60 seconds. Returns the finished object IDs.
const SETTLE_OBJECT_DELETIONS_SQL: &str = "WITH settled AS (SELECT * FROM unnest($2::BIGINT[], $4::BIGINT[], $5::TEXT[], $6::BOOLEAN[]) AS settled(object_id, fence, error, defer)), finished AS (UPDATE snapshot_payload_upload_objects AS uploaded SET closed_at = COALESCE(uploaded.closed_at, clock_timestamp()), next_retry_at = NULL, last_error = settled.error, reconcile_owner = NULL, reconcile_lease_expires_at = NULL FROM settled WHERE uploaded.vol = $1 AND uploaded.object_id = settled.object_id AND uploaded.reconcile_owner = $3::TEXT AND uploaded.reconcile_fence = settled.fence AND uploaded.state = 'delete_pending' RETURNING uploaded.object_id, settled.error IS NULL AS deleted), acknowledged AS (UPDATE snapshot_payload_gc_candidates AS candidate SET acknowledged_at = clock_timestamp(), next_attempt_at = NULL, last_error = NULL FROM finished WHERE candidate.vol = $1 AND candidate.object_id = finished.object_id AND finished.deleted AND candidate.acknowledged_at IS NULL), deferred AS (UPDATE snapshot_payload_gc_candidates AS candidate SET next_attempt_at = clock_timestamp() + interval '60 seconds', last_error = 'cli janitor deletion retry required' FROM settled WHERE candidate.vol = $1 AND candidate.object_id = settled.object_id AND settled.defer AND candidate.acknowledged_at IS NULL AND NOT EXISTS (SELECT 1 FROM finished WHERE finished.object_id = settled.object_id AND finished.deleted)) SELECT object_id FROM finished";

pub(super) enum ObjectDeletionClaimError {
    VolumeBindingMissing,
    Postgres(&'static str, tokio_postgres::Error),
}

/// Claims one object for deletion in `tx` with [`CLAIM_OBJECT_DELETIONS_SQL`] for
/// the mounted Janitor. Returns the fencing token, or `None` when the object is
/// protected.
pub(super) async fn claim_object_deletion_in(
    tx: tokio_postgres::Transaction<'_>,
    volume_id: i64,
    object_id: i64,
    owner: &str,
) -> Result<Option<i64>, ObjectDeletionClaimError> {
    use ObjectDeletionClaimError::Postgres;
    let lease_secs = super::snapshot_reachability::SNAPSHOT_UPLOADED_FRAGMENT_LEASE_SECS as f64;
    tx.query_opt(CLAIM_PUBLICATION_FENCE_SQL, &[&volume_id])
        .await
        .map_err(|error| Postgres("janitor claim publication fence failed", error))?
        .ok_or(ObjectDeletionClaimError::VolumeBindingMissing)?;
    let claimed = tx
        .query_opt(
            CLAIM_OBJECT_DELETIONS_SQL.as_str(),
            &[&volume_id, &vec![object_id], &owner, &lease_secs],
        )
        .await
        .map_err(|error| Postgres("janitor claim lease failed", error))?;
    tx.commit()
        .await
        .map_err(|error| Postgres("janitor claim commit failed", error))?;
    Ok(claimed.map(|row| row.get::<_, i64>(1)))
}

/// Keeps only the SQLSTATE code: the error text may carry connection details, the
/// code cannot. Class 42 (undefined table or column, insufficient privilege) means
/// the schema does not fit this binary, which no retry fixes.
fn janitor_pg_error(context: &'static str, error: tokio_postgres::Error) -> JanitorMetadataError {
    match error.code().map(tokio_postgres::error::SqlState::code) {
        Some(code) if code.starts_with("42") => {
            JanitorMetadataError::SchemaUnavailable(format!("{context} (SQLSTATE {code})"))
        }
        Some(code) => {
            JanitorMetadataError::BackendUnavailable(format!("{context} (SQLSTATE {code})"))
        }
        None => JanitorMetadataError::BackendUnavailable(context.to_owned()),
    }
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

#[cfg(test)]
mod version_gate_tests {
    #[test]
    fn only_a_local_replica_ahead_of_postgres_is_unsafe() {
        assert!(!super::local_is_ahead(5, 5));
        assert!(
            !super::local_is_ahead(5, 9),
            "peers may publish during a cycle"
        );
        assert!(super::local_is_ahead(6, 5));
        assert!(super::local_is_ahead(u64::MAX, i64::MAX));
    }
}

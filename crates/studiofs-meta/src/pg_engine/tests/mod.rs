use super::*;
use crate::allocator_state::{AllocatorRecord, AllocatorStateV1, COUNTER_LIMIT};
use crate::pg_dsn::ensure_studiofs_database;
use crate::pg_engine::inode_allocator::AllocatorRuntime;
use crate::pg_engine::runtime::{PgConnectionPool, PgRemoteRuntime};
use sha2::{Digest, Sha256};
use uuid::Uuid;

mod inode_allocator;
mod janitor;
mod lifecycle;
mod lock_leases;
mod lock_workers;
mod metadata_notify;
mod remote_apply;
mod replication_backlog;
mod runtime;
mod snapshot_control;
mod snapshot_policy;
mod snapshot_reachability;
mod snapshot_replay;

fn pg_err(e: tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!("{} (sqlstate={})", db.message(), db.code().code())
    } else {
        e.to_string()
    }
}

fn workspace_env_path() -> std::path::PathBuf {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(|path| path.parent())
        .map(std::path::Path::to_path_buf)
        .unwrap_or(manifest_dir)
        .join(".env")
}

fn pg_dsn() -> Option<String> {
    let env_path = workspace_env_path();
    if env_path.is_file() {
        dotenvy::from_path(&env_path).expect("failed to load workspace .env");
    } else {
        let _ = dotenvy::dotenv();
    }
    let raw = std::env::var("STUDIOFS_TEST_PG_DSN")
        .ok()
        .filter(|value| !value.trim().is_empty())?;
    Some(
        run_async(async { ensure_studiofs_database(&raw).await })
            .expect("failed to provision STUDIOFS_TEST_PG_DSN database")
            .metadata_dsn,
    )
}

fn run_async<T, F>(fut: F) -> MetaResult<T>
where
    F: std::future::Future<Output = MetaResult<T>>,
{
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    let rt = RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime")
    });
    rt.block_on(fut)
}

trait PgFileGraphFixture {
    fn stage_payload_slice(
        &self,
        ino: Ino,
        block_index: u32,
        payload: &PayloadRef,
    ) -> MetaResult<()>;
    fn stage_payload_tombstone(&self, ino: Ino, block_index: u32) -> MetaResult<()>;
}

fn fixture_payload_slices(
    block_index: u32,
    payload: Option<&PayloadRef>,
    state: PayloadRefState,
) -> MetaResult<Vec<(ChunkId, Vec<SliceRecord>)>> {
    let block_start = u64::from(block_index)
        .checked_mul(u64::from(DEFAULT_BLOCK_SIZE))
        .ok_or_else(|| MetaError::Storage("fixture block offset overflow".to_string()))?;
    let data_end = block_start
        .checked_add(u64::from(payload.map_or(0, |payload| payload.length)))
        .ok_or_else(|| MetaError::Storage("fixture data range overflow".to_string()))?;
    let block_end = block_start
        .checked_add(u64::from(DEFAULT_BLOCK_SIZE))
        .ok_or_else(|| MetaError::Storage("fixture block range overflow".to_string()))?;
    let mut chunks = std::collections::BTreeMap::<ChunkId, Vec<SliceRecord>>::new();
    let mut push = |start: u64, end: u64, source: Option<&PayloadRef>| -> MetaResult<()> {
        let mut cursor = start;
        while cursor < end {
            let chunk_id = chunk_id_from_offset(cursor, DEFAULT_CHUNK_SIZE);
            let chunk_start = chunk_start_offset(chunk_id, DEFAULT_CHUNK_SIZE);
            let piece_end = end.min(chunk_start.saturating_add(DEFAULT_CHUNK_SIZE as u64));
            let range = SliceRange::new(
                (cursor - chunk_start) as u32,
                (piece_end - chunk_start) as u32,
            )
            .ok_or_else(|| MetaError::Storage("fixture invalid slice range".to_string()))?;
            let mut slice = if let Some(source) = source {
                let delta = u32::try_from(cursor.saturating_sub(block_start))
                    .map_err(|_| MetaError::Storage("fixture slice delta overflow".to_string()))?;
                SliceRecord::new(
                    SliceId(0),
                    SliceOrder(0),
                    range,
                    PayloadRef {
                        object_id: source.object_id,
                        offset: source.offset.saturating_add(delta),
                        length: u32::try_from(piece_end.saturating_sub(cursor)).map_err(|_| {
                            MetaError::Storage("fixture slice length overflow".to_string())
                        })?,
                        flags: source.flags,
                    },
                )
            } else {
                SliceRecord::tombstone(SliceId(0), SliceOrder(0), range)
            };
            slice.payload_state = state;
            chunks.entry(chunk_id).or_default().push(slice);
            cursor = piece_end;
        }
        Ok(())
    };
    push(block_start, data_end, payload)?;
    if data_end < block_end {
        push(data_end, block_end, None)?;
    }
    Ok(chunks.into_iter().collect())
}

fn lookup_first_object_id(engine: &PgMetaEngine, ino: Ino) -> MetaResult<Option<u64>> {
    Ok(engine
        .lookup_regular_file_slices(ino, 0, u64::from(DEFAULT_BLOCK_SIZE))?
        .into_iter()
        .find_map(|span| span.payload.map(|block| block.object_id)))
}

fn lookup_object_ids(engine: &PgMetaEngine, ino: Ino, length: u64) -> MetaResult<Vec<u64>> {
    Ok(engine
        .lookup_regular_file_slices(ino, 0, length)?
        .into_iter()
        .filter_map(|span| span.payload.map(|block| block.object_id))
        .collect())
}

fn mark_overlay_objects_uploaded(
    engine: &PgMetaEngine,
    ino: Ino,
    expected_epoch: u64,
    object_ids: &[u64],
) {
    for object_id in object_ids {
        assert!(
            engine
                .mark_regular_file_overlay_object_uploaded_if_matches(
                    ino,
                    expected_epoch,
                    *object_id,
                )
                .expect("mark overlay object uploaded"),
            "object {object_id} should still belong to inode {ino} epoch {expected_epoch}"
        );
    }
}

impl PgFileGraphFixture for PgMetaEngine {
    fn stage_payload_slice(
        &self,
        ino: Ino,
        block_index: u32,
        payload: &PayloadRef,
    ) -> MetaResult<()> {
        self.flush_regular_file_overlay_slices(
            ino,
            fixture_payload_slices(block_index, Some(payload), PayloadRefState::LocalOnly)?
                .as_slice(),
        )?;
        Ok(())
    }

    fn stage_payload_tombstone(&self, ino: Ino, block_index: u32) -> MetaResult<()> {
        self.flush_regular_file_overlay_slices(
            ino,
            fixture_payload_slices(block_index, None, PayloadRefState::LocalOnly)?.as_slice(),
        )?;
        Ok(())
    }
}

fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    cond()
}

fn wait_visible(engine: &PgMetaEngine, parent: Ino, name: &[u8], timeout: Duration) -> bool {
    wait_until(timeout, || {
        let _ = engine.sync_metadata();
        engine.lookup(parent, name).is_ok()
    })
}

fn wait_hidden(engine: &PgMetaEngine, parent: Ino, name: &[u8], timeout: Duration) -> bool {
    wait_until(timeout, || {
        let _ = engine.sync_metadata();
        matches!(
            engine.lookup(parent, name),
            Err(MetaError::EntryNotFound { .. })
        )
    })
}

fn drop_file_lock_lease_table(dsn: &str) {
    run_async(async move {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| MetaError::Storage(format!("postgres connect failed: {}", pg_err(e))))?;
        let conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .execute("DROP TABLE IF EXISTS file_lock_leases", &[])
            .await
            .map_err(|e| {
                MetaError::Storage(format!(
                    "postgres drop file_lock_leases failed: {}",
                    pg_err(e)
                ))
            })?;
        conn_task.abort();
        Ok::<(), MetaError>(())
    })
    .expect("drop file_lock_leases");
}

fn send_pg_notify(dsn: &str, channel: &str, payload: &str) {
    run_async(async move {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| MetaError::Storage(format!("postgres connect failed: {}", pg_err(e))))?;
        let conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .execute("SELECT pg_notify($1, $2)", &[&channel, &payload])
            .await
            .map_err(|e| MetaError::Storage(format!("postgres pg_notify failed: {}", pg_err(e))))?;
        conn_task.abort();
        Ok::<(), MetaError>(())
    })
    .expect("send pg notify");
}

static FILE_LOCK_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

fn file_lock_test_guard() -> std::sync::MutexGuard<'static, ()> {
    FILE_LOCK_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("file lock test serial guard")
}

fn reopen_pg_engine_with_retry(path: &std::path::Path) -> Arc<PgMetaEngine> {
    let mut reopened = None;
    assert!(wait_until(Duration::from_secs(3), || {
        match PgMetaEngine::open(path) {
            Ok(engine) => {
                reopened = Some(engine);
                true
            }
            Err(_) => false,
        }
    }));
    reopened.expect("reopen pg engine")
}

struct PgTestVolume {
    dsn: String,
    name: String,
    uuid: String,
    volume_id: i64,
}

impl PgTestVolume {
    fn new(dsn: &str, prefix: &str) -> Self {
        let name = format!(
            "{prefix}-{}-{}",
            std::process::id(),
            Uuid::new_v4().simple()
        );
        let uuid = Uuid::new_v4().to_string();
        let volume_id = setup_pg_volume(dsn, &name, &uuid, DEFAULT_BLOCK_SIZE).expect("setup pg");
        Self {
            dsn: dsn.to_string(),
            name,
            uuid,
            volume_id,
        }
    }

    fn format_info(&self) -> FormatInfo {
        format_info(&self.name, &self.uuid, &self.dsn, self.volume_id)
    }

    fn update_uuid(&self, new_uuid: &str) {
        let _ = run_async(async {
            let (client, connection) = tokio_postgres::connect(&self.dsn, tokio_postgres::NoTls)
                .await
                .expect("connect");
            let conn_task = tokio::spawn(async move {
                let _ = connection.await;
            });
            client
                .execute(
                    "UPDATE volumes SET uuid = $1 WHERE vol = $2",
                    &[&new_uuid, &self.volume_id],
                )
                .await
                .expect("update uuid");
            conn_task.abort();
            Ok::<(), MetaError>(())
        });
    }
}

impl Drop for PgTestVolume {
    fn drop(&mut self) {
        let _ = run_async(async {
            let (client, connection) = tokio_postgres::connect(&self.dsn, tokio_postgres::NoTls)
                .await
                .map_err(|e| {
                    MetaError::Storage(format!("postgres connect failed: {}", pg_err(e)))
                })?;
            let conn_task = tokio::spawn(async move {
                let _ = connection.await;
            });
            client
                .execute("DELETE FROM volumes WHERE vol = $1", &[&self.volume_id])
                .await
                .map_err(|e| {
                    MetaError::Storage(format!("postgres cleanup failed: {}", pg_err(e)))
                })?;
            conn_task.abort();
            Ok::<(), MetaError>(())
        });
    }
}

fn seed_local(path: &std::path::Path, info: &FormatInfo) {
    let seed = RedbMetaEngine::open(path).expect("seed open");
    seed.set_format_info(info).expect("set format");
    seed.init_root(0, 0).expect("init root");
    let next_event_seq = seed.get_counter("nextEventSeq").expect("nextEventSeq");
    seed.set_counter(LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER, next_event_seq)
        .expect("seed flushed event seq");
}

fn set_file_state(engine: &PgMetaEngine, ino: Ino, size: u64, content_epoch: u64) {
    let mut attr = engine.get_attr(ino).expect("get file attr");
    attr.size = size;
    attr.content_epoch = content_epoch;
    attr.mtime = InodeAttr::now_nanos();
    attr.ctime = attr.mtime;
    engine.set_attr(&attr).expect("set file attr");
}

fn create_published_small_file(
    engine: &PgMetaEngine,
    parent: Ino,
    name: &[u8],
    object_id: u64,
) -> InodeAttr {
    let file = engine
        .create_file(parent, name, 0, 0, 0o644)
        .expect("create published file");
    set_file_state(engine, file.ino, 64, 1);
    engine
        .stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(object_id, 64))
        .expect("set published payload slice");
    mark_overlay_objects_uploaded(engine, file.ino, 1, &[object_id]);
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish small file"),
        "small file publish must succeed"
    );
    file
}

pub(crate) fn temp_pg_engine_for_unit() -> (tempfile::TempDir, PgMetaEngine) {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let path = dir.path().join("meta.redb");
    let local = Arc::new(RedbMetaEngine::open(&path).expect("open local redb"));
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "unit-local",
            "unit-local-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set local format info");
    local
        .set_remote_namespace_visibility_filter(true)
        .expect("enable remote namespace visibility filter");
    let flushed_event_seq = local.get_counter("nextEventSeq").expect("nextEventSeq");

    let metadata_invalidation_notify = Arc::clone(&local.invalidation_notify);
    let engine = PgMetaEngine {
        local,
        postgres_url: "postgres://127.0.0.1:1/studiofs".to_string(),
        volume_id: 1,
        client_id: "unit-test".to_string(),
        client_slot_plus_one: AtomicU64::new(0),
        local_version: AtomicI64::new(0),
        namespace_version: AtomicI64::new(0),
        flushed_event_seq: AtomicU64::new(flushed_event_seq),
        local_position_lock: Mutex::new(()),
        replication_lock: Mutex::new(()),
        poll_interval: Duration::from_secs(60),
        flush_interval: Duration::from_secs(60),
        flush_idle_grace: Duration::from_secs(60),
        flush_ops_threshold: 8,
        connect_timeout: Duration::from_millis(50),
        command_timeout: Duration::from_millis(50),
        command_pool: Arc::new(
            PgConnectionPool::new(
                "postgres://127.0.0.1:1/studiofs".to_string(),
                Duration::from_millis(50),
                Duration::from_millis(50),
                4,
            )
            .expect("non-zero command pool size"),
        ),
        inflight_replication_ops: AtomicU32::new(0),
        pending_state: Mutex::new(PendingReplicationState::default()),
        remote_runtime: PgRemoteRuntime::new("studiofs-pg-remote-runtime-test")
            .expect("unit test remote runtime"),
        allocator_runtime: Mutex::new(AllocatorRuntime::default()),
        allocator_refill_lock: Mutex::new(()),
        inode_id_headroom_available: AtomicBool::new(false),
        object_id_headroom_available: AtomicBool::new(false),
        janitor_lock: Mutex::new(None),
        file_lock_cache: Mutex::new(HashMap::new()),
        held_file_locks: Mutex::new(HashMap::new()),
        active_open_inodes: Mutex::new(HashMap::new()),
        self_weak: OnceLock::new(),
        lock_workers_enabled: false,
        lock_worker_start_lock: Mutex::new(()),
        background_workers: PgBackgroundWorkers::new(),
        metadata_invalidation_notify,
        metadata_notify_received: AtomicU64::new(0),
        metadata_notify_reconnects: AtomicU64::new(0),
        metadata_fallback_poll_discoveries: AtomicU64::new(0),
        metadata_sync_errors: AtomicU64::new(0),
        metadata_last_successful_sync_unix_ms: AtomicI64::new(0),
        metadata_remote_head_version: AtomicI64::new(-1),
        metadata_source_connected: AtomicBool::new(false),
    };

    PgMetaEngine::persist_client_slot(&engine.local, 0).expect("persist unit test client slot");
    let allocator_state = AllocatorStateV1::new(
        AllocatorRecord::new(1, 0).expect("inode namespace"),
        AllocatorRecord::new(2, 0).expect("payload namespace"),
    )
    .expect("allocator state");
    engine
        .local
        .replace_allocator_state(allocator_state)
        .expect("persist unit test allocator state");
    engine.bootstrap_allocator_state();

    (dir, engine)
}

fn apply_projected_remote_range(
    writer: &PgMetaEngine,
    remote: &PgMetaEngine,
    start: u64,
    end: u64,
) {
    let events = writer
        .local
        .read_events_range(start, end)
        .expect("read local events");
    let projected = RemoteApplyCoordinator::project_remote_events(&events);
    apply_foreign_batch(
        remote,
        &projected,
        &OverlayConflictSets::default(),
        &HashSet::new(),
    )
    .expect("apply projected remote batch");
}

fn apply_foreign_batch(
    pg: &PgMetaEngine,
    batch_events: &[MetaEvent],
    overlay: &OverlayConflictSets,
    base_protected_regular_file_inodes: &HashSet<Ino>,
) -> MetaResult<(bool, OverlayConflictSets)> {
    let outcome = RemoteApplyCoordinator::new(&pg.local).apply_foreign_batch(
        batch_events,
        overlay,
        base_protected_regular_file_inodes,
    )?;
    Ok((outcome.changed, outcome.protected_overlay))
}

fn apply_visible_self_batch(pg: &PgMetaEngine, batch_events: &[MetaEvent]) -> MetaResult<bool> {
    RemoteApplyCoordinator::new(&pg.local).apply_visible_self_batch(batch_events)
}

fn acknowledge_local_prefix_from_self_batch(
    pg: &PgMetaEngine,
    batch_events: &[MetaEvent],
) -> MetaResult<()> {
    let flushed_event_seq = pg.flushed_event_seq.load(Ordering::SeqCst);
    let next_event_seq = pg.local.get_counter("nextEventSeq")?;
    if let Some(next_flushed_event_seq) = RemoteApplyCoordinator::new(&pg.local)
        .acknowledge_local_prefix_from_self_batch(batch_events, flushed_event_seq, next_event_seq)?
    {
        pg.persist_flushed_event_seq(next_flushed_event_seq)?;
    }
    Ok(())
}

fn apply_decoded_foreign_batches_locked(
    pg: &PgMetaEngine,
    batches: Vec<DecodedReplayBatch>,
    recent_flush: Option<&RecentFlush>,
    last_seq: i64,
) -> MetaResult<bool> {
    let coordinator = RemoteApplyCoordinator::new(&pg.local);
    let flushed_event_seq = pg.flushed_event_seq.load(Ordering::SeqCst);
    let next_event_seq = pg.local.get_counter("nextEventSeq")?;
    let initial_overlay = pg.local_overlay_conflicts()?;
    let outcome = coordinator.apply_decoded_batches(
        batches,
        recent_flush,
        RemoteReplayApplyContext {
            last_seq,
            current_namespace_version: pg.namespace_version.load(Ordering::SeqCst),
            flushed_event_seq,
            next_event_seq,
            initial_overlay,
        },
    )?;
    pg.persist_replay_position(&outcome)?;
    Ok(outcome.changed)
}

fn reconcile_recent_flush_regular_files(
    pg: &PgMetaEngine,
    recent_flush: Option<&RecentFlush>,
) -> MetaResult<bool> {
    let overlay = pg.local_overlay_conflicts()?;
    RemoteApplyCoordinator::new(&pg.local).reconcile_recent_flush(
        recent_flush,
        pg.local_version.load(Ordering::SeqCst),
        &overlay,
    )
}

const SETUP_PG_VOLUME_ADVISORY_LOCK_ID: i64 = 902_202_603;

fn setup_pg_volume(dsn: &str, name: &str, uuid: &str, block_size: u32) -> MetaResult<i64> {
    run_async(async move {
        let (mut client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| MetaError::Storage(format!("postgres connect failed: {}", pg_err(e))))?;
        let conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });

        let tx = client
            .transaction()
            .await
            .map_err(|e| MetaError::Storage(format!("postgres begin tx failed: {}", pg_err(e))))?;

        tx.execute(
            "SELECT pg_advisory_xact_lock($1)",
            &[&SETUP_PG_VOLUME_ADVISORY_LOCK_ID],
        )
        .await
        .map_err(|e| MetaError::Storage(format!("postgres advisory lock failed: {}", pg_err(e))))?;

        crate::pg_engine::schema::ensure_studiofs_pg_schema(&tx).await?;

        let row = tx
            .query_one(
                r#"
                    INSERT INTO volumes (name, uuid, block_size)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (uuid) DO UPDATE
                    SET name = EXCLUDED.name,
                        block_size = EXCLUDED.block_size
                    RETURNING vol
                    "#,
                &[&name, &uuid, &(block_size as i32)],
            )
            .await
            .map_err(|e| {
                MetaError::Storage(format!("postgres volume upsert failed: {}", pg_err(e)))
            })?;
        let volume_id: i64 = row.get(0);

        let empty_checkpoint = postcard::to_stdvec(&crate::snapshot::ServingCheckpoint::default())
            .map_err(|e| MetaError::Serialization(format!("snapshot encode failed: {e}")))?;
        let checkpoint_sha256 = Sha256::digest(&empty_checkpoint).to_vec();
        let metadata_format_version = CURRENT_FORMAT_VERSION as i16;
        let format_info_bytes = serde_json::to_vec(&format_info(name, uuid, dsn, volume_id))
            .map_err(|e| MetaError::Serialization(format!("format info encode failed: {e}")))?;
        tx.execute(
            r#"
                INSERT INTO metadata_state (
                    vol,
                    metadata_format_version,
                    version,
                    checkpoint_version,
                    snapshot,
                    checkpoint_sha256,
                    format_info,
                    checkpoint_publication_state,
                    checkpoint_generation
                )
                VALUES ($1, $2, 0, 0, $3, $4, $5, 'published', 1)
                ON CONFLICT (vol) DO NOTHING
                "#,
            &[
                &volume_id,
                &metadata_format_version,
                &empty_checkpoint,
                &checkpoint_sha256,
                &format_info_bytes,
            ],
        )
        .await
        .map_err(|e| {
            MetaError::Storage(format!(
                "postgres metadata_state seed failed: {}",
                pg_err(e)
            ))
        })?;

        tx.commit()
            .await
            .map_err(|e| MetaError::Storage(format!("postgres commit failed: {}", pg_err(e))))?;
        conn_task.abort();
        Ok(volume_id)
    })
}

fn format_info(name: &str, uuid: &str, dsn: &str, volume_id: i64) -> FormatInfo {
    FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: name.to_string(),
        uuid: uuid.to_string(),
        endpoint: "http://127.0.0.1:9000".to_string(),
        bucket: "studiofs-test".to_string(),
        access_key: "test".to_string(),
        secret_key: "test".to_string(),
        region: "us-east-1".to_string(),
        data_prefix: format!("{name}/objects/"),
        metadata: MetadataConfig {
            backend: MetadataBackend::Postgres,
            remote: None,
            postgres: Some(PostgresConfig {
                url: dsn.to_string(),
                volume_id,
            }),
        },
        encryption: EncryptionConfig::new([0xA5; ENCRYPTION_KEY_LENGTH]),
        block_size: DEFAULT_BLOCK_SIZE,
    }
}

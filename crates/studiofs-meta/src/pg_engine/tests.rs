use super::*;
use crate::pg_dsn::ensure_studiofs_database;
use std::cell::RefCell;
use std::collections::VecDeque;
use tempfile::tempdir;
use uuid::Uuid;

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
        let _ = dotenvy::from_path(&env_path);
    } else {
        let _ = dotenvy::dotenv();
    }
    let raw = std::env::var("STUDIOFS_TEST_PG_DSN")
        .ok()
        .filter(|v| !v.trim().is_empty())?;
    run_async(async { ensure_studiofs_database(&raw).await })
        .ok()
        .map(|v| v.metadata_dsn)
}

fn run_async<T, F>(fut: F) -> MetaResult<T>
where
    F: std::future::Future<Output = MetaResult<T>>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| MetaError::Storage(format!("failed to build tokio runtime: {e}")))?;
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

thread_local! {
    static TEST_SCHEMA_ENSURE_OVERRIDE: RefCell<Option<String>> = const { RefCell::new(None) };
    static TEST_INODE_WINDOW_RESERVATION_OVERRIDE: RefCell<VecDeque<Result<InodeWindow, String>>> =
        const { RefCell::new(VecDeque::new()) };
}

struct TestSchemaEnsureOverrideGuard {
    previous: Option<String>,
}

impl Drop for TestSchemaEnsureOverrideGuard {
    fn drop(&mut self) {
        TEST_SCHEMA_ENSURE_OVERRIDE.with(|override_state| {
            *override_state.borrow_mut() = self.previous.take();
        });
    }
}

fn set_schema_ensure_test_override(message: &str) -> TestSchemaEnsureOverrideGuard {
    let previous = TEST_SCHEMA_ENSURE_OVERRIDE
        .with(|override_state| override_state.borrow_mut().replace(message.to_string()));
    TestSchemaEnsureOverrideGuard { previous }
}

pub(super) fn schema_ensure_test_override() -> Option<String> {
    TEST_SCHEMA_ENSURE_OVERRIDE.with(|override_state| override_state.borrow().clone())
}

pub(super) fn headroom_refresh_test_override() -> Option<bool> {
    None
}

struct TestInodeWindowReservationOverrideGuard {
    previous: VecDeque<Result<InodeWindow, String>>,
}

impl Drop for TestInodeWindowReservationOverrideGuard {
    fn drop(&mut self) {
        TEST_INODE_WINDOW_RESERVATION_OVERRIDE.with(|override_state| {
            *override_state.borrow_mut() = std::mem::take(&mut self.previous);
        });
    }
}

fn set_inode_window_reservation_override(
    windows: impl IntoIterator<Item = Result<InodeWindow, String>>,
) -> TestInodeWindowReservationOverrideGuard {
    let previous = TEST_INODE_WINDOW_RESERVATION_OVERRIDE.with(|override_state| {
        std::mem::replace(
            &mut *override_state.borrow_mut(),
            windows.into_iter().collect(),
        )
    });
    TestInodeWindowReservationOverrideGuard { previous }
}

pub(super) fn pop_inode_window_reservation_override() -> Option<Result<InodeWindow, String>> {
    TEST_INODE_WINDOW_RESERVATION_OVERRIDE
        .with(|override_state| override_state.borrow_mut().pop_front())
}

struct TestEnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl Drop for TestEnvGuard {
    fn drop(&mut self) {
        if let Some(previous) = &self.previous {
            unsafe {
                std::env::set_var(self.key, previous);
            }
        } else {
            unsafe {
                std::env::remove_var(self.key);
            }
        }
    }
}

fn set_test_env(key: &'static str, value: &str) -> TestEnvGuard {
    let previous = std::env::var(key).ok();
    unsafe {
        std::env::set_var(key, value);
    }
    TestEnvGuard { key, previous }
}

#[test]
fn project_remote_events_filters_unpublished_overlay_slices() {
    let events = vec![
        MetaEvent::SetRegularFileOverlaySlices {
            ino: 42,
            chunk_slices: vec![(
                ChunkId(0),
                vec![SliceRecord {
                    slice_id: SliceId(0),
                    order: SliceOrder(0),
                    range: SliceRange::new(0, 4096).unwrap(),
                    payload: Some(PayloadRef::new_partial(9001, 4096)),
                    payload_state: PayloadRefState::LocalOnly,
                }],
            )],
        },
        MetaEvent::RemoveRegularFileGraph { ino: 42 },
        MetaEvent::PublishRegularFileBaseSlices {
            ino: 42,
            chunk_slices: vec![(
                ChunkId(0),
                vec![SliceRecord {
                    slice_id: SliceId(1),
                    order: SliceOrder(0),
                    range: SliceRange::new(0, 4096).unwrap(),
                    payload: Some(PayloadRef::new_partial(9002, 4096)),
                    payload_state: PayloadRefState::Published,
                }],
            )],
        },
    ];

    let projected = RemoteApplyCoordinator::project_remote_events(&events);
    assert_eq!(projected.len(), 1);
    assert!(matches!(
        projected.first(),
        Some(MetaEvent::PublishRegularFileBaseSlices { ino: 42, .. })
    ));
}

#[test]
fn self_batch_acknowledges_projected_prefix_with_filtered_overlay_events() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"projected-self-ack.bin", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 4096, 1);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack setup events");

    let payload = PayloadRef::new_partial(77_004, 4096);
    let start = pg.flushed_event_seq.load(Ordering::SeqCst);
    pg.stage_payload_slice(file.ino, 0, &payload)
        .expect("stage local overlay");
    assert!(
        pg.mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 1, payload.object_id)
            .expect("mark uploaded"),
        "upload marker must update the local overlay"
    );
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish base graph"),
        "base graph publish must succeed"
    );
    let end = pg.local.get_counter("nextEventSeq").expect("event end");
    let events = pg
        .local
        .read_events_range(start, end)
        .expect("read local events");
    let projected = RemoteApplyCoordinator::project_remote_events(&events);
    assert!(
        events.len() > projected.len(),
        "setup should include local-only overlay events before the remote-visible publish"
    );

    pg.flushed_event_seq.store(start, Ordering::SeqCst);
    acknowledge_local_prefix_from_self_batch(&pg, &projected).expect("ack projected self batch");

    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        end,
        "self-batch ack must advance over the raw local events that produced the projected batch"
    );
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

fn temp_pg_engine_for_unit() -> (tempfile::TempDir, PgMetaEngine) {
    let dir = tempdir().expect("tempdir");
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

    let engine = PgMetaEngine {
        local,
        postgres_url: "postgres://127.0.0.1:1/studiofs".to_string(),
        volume_id: 1,
        client_id: "unit-test".to_string(),
        client_slot_plus_one: AtomicU64::new(0),
        local_version: AtomicI64::new(0),
        namespace_version: AtomicI64::new(0),
        flushed_event_seq: AtomicU64::new(flushed_event_seq),
        replication_lock: Mutex::new(()),
        poll_interval: Duration::from_secs(60),
        flush_interval: Duration::from_secs(60),
        flush_idle_grace: Duration::from_secs(60),
        flush_ops_threshold: 8,
        connect_timeout: Duration::from_millis(50),
        command_timeout: Duration::from_millis(50),
        inflight_replication_ops: AtomicU32::new(0),
        pending_state: Mutex::new(PendingReplicationState::default()),
        inode_allocator_lock: Mutex::new(()),
        inode_window_reservation_lock: Mutex::new(()),
        inode_id_headroom_available: AtomicBool::new(false),
        janitor_lock: Mutex::new(None),
        file_lock_cache: Mutex::new(HashMap::new()),
        held_file_locks: Mutex::new(HashMap::new()),
        self_weak: OnceLock::new(),
        lock_workers_enabled: false,
        lock_worker_start_lock: Mutex::new(()),
        poll_shutdown: Arc::new(AtomicBool::new(false)),
        poll_wakeup: Arc::new((Mutex::new(false), Condvar::new())),
        lock_notify_cancel: CancellationToken::new(),
        metadata_invalidation_notify: Arc::new(tokio::sync::Notify::new()),
        metadata_notify_cancel: CancellationToken::new(),
        metadata_notify_received: AtomicU64::new(0),
        metadata_notify_reconnects: AtomicU64::new(0),
        metadata_fallback_poll_discoveries: AtomicU64::new(0),
        metadata_sync_errors: AtomicU64::new(0),
        metadata_last_successful_sync_unix_ms: AtomicI64::new(0),
        poll_handle: Mutex::new(None),
        metadata_notify_handle: Mutex::new(None),
        lock_notify_handle: Mutex::new(None),
        lock_renew_handle: Mutex::new(None),
    };

    PgMetaEngine::persist_client_slot(&engine.local, 0).expect("persist unit test client slot");
    let inode_state = InodeAllocatorState {
        current: InodeWindow {
            start: ROOT_INO + 1,
            next: ROOT_INO + 1,
            end: ROOT_INO + 1 + PG_INODE_WINDOW_SIZE,
        },
        prefetched: InodeWindow::default(),
    };
    engine
        .persist_inode_allocator_state(inode_state)
        .expect("persist unit test inode window");
    engine.update_inode_headroom_flag(inode_state);

    (dir, engine)
}

#[test]
fn prepare_for_close_does_not_block_forever_on_slow_background_thread() {
    let _join_timeout = set_test_env("STUDIOFS_PG_PREPARE_FOR_CLOSE_JOIN_TIMEOUT_MS", "50");
    let (_dir, engine) = temp_pg_engine_for_unit();
    *engine.poll_handle.lock().expect("poll_handle lock") = Some(
        std::thread::Builder::new()
            .name("pg-test-slow-close".into())
            .spawn(|| {
                std::thread::sleep(Duration::from_secs(5));
            })
            .expect("spawn slow background thread"),
    );

    let started = Instant::now();
    engine.prepare_for_close();

    assert!(
        started.elapsed() < Duration::from_secs(1),
        "prepare_for_close should respect the bounded join timeout for slow threads"
    );
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
    let outcome = RemoteApplyCoordinator::new(pg).apply_foreign_batch(
        batch_events,
        overlay,
        base_protected_regular_file_inodes,
    )?;
    Ok((outcome.changed, outcome.protected_overlay))
}

fn apply_visible_self_batch(pg: &PgMetaEngine, batch_events: &[MetaEvent]) -> MetaResult<bool> {
    RemoteApplyCoordinator::new(pg).apply_visible_self_batch(batch_events)
}

fn acknowledge_local_prefix_from_self_batch(
    pg: &PgMetaEngine,
    batch_events: &[MetaEvent],
) -> MetaResult<()> {
    RemoteApplyCoordinator::new(pg).acknowledge_local_prefix_from_self_batch(batch_events)
}

fn apply_decoded_foreign_batches_locked(
    pg: &PgMetaEngine,
    batches: Vec<DecodedReplayBatch>,
    recent_flush: Option<&RecentFlush>,
    last_seq: i64,
) -> MetaResult<bool> {
    let coordinator = RemoteApplyCoordinator::new(pg);
    let outcome = coordinator.apply_decoded_batches(batches, recent_flush, last_seq)?;
    coordinator.persist_replay_outcome(&outcome)?;
    Ok(outcome.changed)
}

fn reconcile_recent_flush_regular_files(
    pg: &PgMetaEngine,
    recent_flush: Option<&RecentFlush>,
) -> MetaResult<bool> {
    RemoteApplyCoordinator::new(pg).reconcile_recent_flush(recent_flush)
}

#[test]
fn foreign_unlink_after_local_rename_removes_destination_alias() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"file.txt", 77_010);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack published setup");

    pg.rename(ROOT_INO, b"file.txt", ROOT_INO, b"renamed.txt")
        .expect("local rename");
    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(pg.lookup(ROOT_INO, b"renamed.txt").is_ok());

    apply_foreign_batch(
        &pg,
        &[
            MetaEvent::RemoveEntry {
                parent: ROOT_INO,
                name: b"file.txt".to_vec(),
            },
            MetaEvent::RemoveAttr { ino: file.ino },
        ],
        &overlay,
        &HashSet::new(),
    )
    .expect("apply foreign unlink");

    assert!(pg.lookup(ROOT_INO, b"file.txt").is_err());
    assert!(pg.lookup(ROOT_INO, b"renamed.txt").is_err());
    assert!(pg.get_attr(file.ino).is_err());
}

#[test]
fn foreign_unlink_during_local_append_removes_serving_dentry() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"append-target.txt", 77_011);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack published setup");

    set_file_state(&pg, file.ino, 64 + u64::from(DEFAULT_BLOCK_SIZE), 2);
    let local_tail = PayloadRef::new_partial(77_012, DEFAULT_BLOCK_SIZE);
    pg.stage_payload_slice(file.ino, 1, &local_tail)
        .expect("stage local append overlay");
    let local_append_start = pg.flushed_event_seq.load(Ordering::SeqCst);
    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(overlay.block_inodes.contains(&file.ino));
    assert!(pg.lookup(ROOT_INO, b"append-target.txt").is_ok());
    let batch = [
        MetaEvent::RemoveEntry {
            parent: ROOT_INO,
            name: b"append-target.txt".to_vec(),
        },
        MetaEvent::RemoveAttr { ino: file.ino },
    ];
    let current_regular_file_conflicts = RemoteApplyCoordinator::new(&pg)
        .current_regular_file_overlay_conflicts_for_batch(&batch)
        .expect("collect current regular-file conflicts");
    assert!(
        current_regular_file_conflicts
            .block_inodes
            .contains(&file.ino)
    );
    assert!(
        current_regular_file_conflicts.dentry_keys.is_empty(),
        "regular-file overlay protection must not mask the foreign unlink path"
    );

    apply_foreign_batch(&pg, &batch, &overlay, &HashSet::new()).expect("apply foreign unlink");

    assert!(
        matches!(
            pg.lookup(ROOT_INO, b"append-target.txt"),
            Err(MetaError::EntryNotFound { .. })
        ),
        "foreign unlink must remove namespace visibility even when local append state is preserved"
    );
    assert!(
        pg.get_attr(file.ino).is_ok(),
        "accepted local payload state may remain observable by inode"
    );

    mark_overlay_objects_uploaded(&pg, file.ino, 2, &[local_tail.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 2)
            .expect("publish local append"),
        "local append publish should still succeed by inode"
    );
    let local_append_end = pg.local.get_counter("nextEventSeq").expect("event end");
    let local_events = pg
        .local
        .read_events_range(local_append_start, local_append_end)
        .expect("read local append events");
    let projected_self_batch = RemoteApplyCoordinator::project_remote_events(&local_events);
    acknowledge_local_prefix_from_self_batch(&pg, &projected_self_batch)
        .expect("ack local append self batch");

    assert!(
        matches!(
            pg.lookup(ROOT_INO, b"append-target.txt"),
            Err(MetaError::EntryNotFound { .. })
        ),
        "self-ack of the local append must not resurrect the deleted path"
    );
}

#[test]
fn foreign_rename_replace_during_local_append_removes_source_dentry() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let source = create_published_small_file(&pg, ROOT_INO, b"slot-3.txt", 77_112);
    let target = create_published_small_file(&pg, ROOT_INO, b"slot-3-renamed.txt", 77_113);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack published setup");

    set_file_state(&pg, source.ino, 227, 2);
    let local_tail = PayloadRef::new_partial(77_114, 163);
    pg.stage_payload_slice(source.ino, 1, &local_tail)
        .expect("stage local append overlay");
    let local_append_start = pg.flushed_event_seq.load(Ordering::SeqCst);
    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(overlay.block_inodes.contains(&source.ino));
    assert!(pg.lookup(ROOT_INO, b"slot-3.txt").is_ok());

    let rename_replace = MetaEvent::RenameEntry {
        parent: ROOT_INO,
        name: b"slot-3.txt".to_vec(),
        new_parent: ROOT_INO,
        new_name: b"slot-3-renamed.txt".to_vec(),
        entry: DirEntryValue {
            ino: source.ino,
            kind: FileType::RegularFile,
        },
        displaced_set_attr: Box::new(None),
        displaced_remove_attr_ino: Some(target.ino),
        moved_dir_attr: Box::new(None),
        old_parent_attr: Box::new(None),
        new_parent_attr: Box::new(None),
    };

    apply_foreign_batch(&pg, &[rename_replace], &overlay, &HashSet::new())
        .expect("apply foreign rename replace");

    assert!(
        matches!(
            pg.lookup(ROOT_INO, b"slot-3.txt"),
            Err(MetaError::EntryNotFound { .. })
        ),
        "foreign rename/replace must remove the old source path even with local append state"
    );
    let renamed = pg
        .lookup(ROOT_INO, b"slot-3-renamed.txt")
        .expect("lookup renamed target");
    assert_eq!(renamed.ino, source.ino);
    assert!(pg.get_attr(target.ino).is_err());

    mark_overlay_objects_uploaded(&pg, source.ino, 2, &[local_tail.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(source.ino, 2)
            .expect("publish local append"),
        "local append publish should still succeed by inode after source rename"
    );
    let local_append_end = pg.local.get_counter("nextEventSeq").expect("event end");
    let local_events = pg
        .local
        .read_events_range(local_append_start, local_append_end)
        .expect("read local append events");
    let projected_self_batch = RemoteApplyCoordinator::project_remote_events(&local_events);
    acknowledge_local_prefix_from_self_batch(&pg, &projected_self_batch)
        .expect("ack local append self batch");

    assert!(
        matches!(
            pg.lookup(ROOT_INO, b"slot-3.txt"),
            Err(MetaError::EntryNotFound { .. })
        ),
        "self-ack of the local append must not resurrect the renamed source path"
    );
    let renamed = pg
        .lookup(ROOT_INO, b"slot-3-renamed.txt")
        .expect("lookup renamed target after self-ack");
    assert_eq!(renamed.ino, source.ino);
}

#[test]
fn foreign_unlink_during_local_uploading_preserves_writer_dentry() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"uploading-target.txt", 77_014);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack published setup");

    set_file_state(&pg, file.ino, 64, 2);
    let local_payload = PayloadRef::new_partial(77_015, 64);
    pg.stage_payload_slice(file.ino, 0, &local_payload)
        .expect("stage active local upload overlay");
    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(overlay.block_inodes.contains(&file.ino));
    assert!(pg.lookup(ROOT_INO, b"uploading-target.txt").is_ok());

    let batch = [
        MetaEvent::RemoveEntry {
            parent: ROOT_INO,
            name: b"uploading-target.txt".to_vec(),
        },
        MetaEvent::RemoveAttr { ino: file.ino },
    ];
    let current_regular_file_conflicts = RemoteApplyCoordinator::new(&pg)
        .current_regular_file_overlay_conflicts_for_batch(&batch)
        .expect("collect current regular-file conflicts");
    assert!(
        current_regular_file_conflicts
            .dentry_keys
            .contains(&PgMetaEngine::dentry_key(ROOT_INO, b"uploading-target.txt")),
        "active writer-local uploads must keep the serving dentry private"
    );

    apply_foreign_batch(&pg, &batch, &overlay, &HashSet::new()).expect("apply foreign unlink");

    assert!(
        pg.lookup(ROOT_INO, b"uploading-target.txt").is_ok(),
        "foreign unlink must not remove an active writer-local uploading path"
    );
    let local_attr = pg.get_attr(file.ino).expect("local attr after unlink");
    assert_eq!(local_attr.upload_state, UploadState::Ready);
    assert_eq!(
        lookup_first_object_id(&pg, file.ino)
            .expect("lookup local payload")
            .expect("local payload remains visible"),
        local_payload.object_id
    );
}

#[test]
fn foreign_unlink_during_unpublished_local_uploading_preserves_writer_dentry() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"new-uploading-target.txt", 0, 0, 0o644)
        .expect("create unpublished local file");
    set_file_state(&pg, file.ino, 64, 1);
    let local_payload = PayloadRef::new_partial(77_016, 64);
    pg.stage_payload_slice(file.ino, 0, &local_payload)
        .expect("stage unpublished local upload overlay");
    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(overlay.block_inodes.contains(&file.ino));
    assert!(pg.lookup(ROOT_INO, b"new-uploading-target.txt").is_ok());

    let batch = [MetaEvent::RemoveEntry {
        parent: ROOT_INO,
        name: b"new-uploading-target.txt".to_vec(),
    }];
    let current_regular_file_conflicts = RemoteApplyCoordinator::new(&pg)
        .current_regular_file_overlay_conflicts_for_batch(&batch)
        .expect("collect current regular-file conflicts");
    assert!(
        current_regular_file_conflicts
            .dentry_keys
            .contains(&PgMetaEngine::dentry_key(
                ROOT_INO,
                b"new-uploading-target.txt"
            )),
        "unpublished writer-local uploads must keep the serving dentry private"
    );

    apply_foreign_batch(&pg, &batch, &overlay, &HashSet::new()).expect("apply foreign unlink");

    assert!(
        pg.lookup(ROOT_INO, b"new-uploading-target.txt").is_ok(),
        "foreign unlink must not remove an unpublished writer-local uploading path"
    );
}

#[test]
fn recent_flush_dentry_reconcile_applies_deferred_foreign_base_entry() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let local_file = pg
        .create_file(ROOT_INO, b"slot.txt", 0, 0, 0o644)
        .expect("create local slot");
    let local_end = pg.local.get_counter("nextEventSeq").expect("local end");
    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    let slot_key = PgMetaEngine::dentry_key(ROOT_INO, b"slot.txt");
    assert!(
        overlay.dentry_keys.contains(&slot_key),
        "local create should protect the serving dentry key"
    );

    let remote_ino = 77_017;
    let mut remote_attr = InodeAttr::new_file(remote_ino, ROOT_INO, 0, 0, 0o644);
    remote_attr.size = 1;
    remote_attr.committed_size = 1;
    remote_attr.content_epoch = 1;
    remote_attr.committed_content_epoch = 1;
    let remote_entry = DirEntryValue {
        ino: remote_ino,
        kind: FileType::RegularFile,
    };
    let remote_batch = [
        MetaEvent::SetAttr {
            attr: remote_attr.clone(),
        },
        MetaEvent::AddEntry {
            parent: ROOT_INO,
            name: b"slot.txt".to_vec(),
            entry: remote_entry,
        },
    ];

    apply_foreign_batch(&pg, &remote_batch, &overlay, &HashSet::new())
        .expect("apply deferred foreign dentry");
    assert_eq!(
        pg.lookup(ROOT_INO, b"slot.txt")
            .expect("local slot still visible")
            .ino,
        local_file.ino,
        "conflicting foreign AddEntry must not replace the local serving key yet"
    );

    pg.persist_flushed_event_seq(local_end)
        .expect("ack local create");
    let overlay_after_ack = pg.local_overlay_conflicts().expect("overlay after ack");
    assert!(
        !overlay_after_ack.dentry_keys.contains(&slot_key),
        "the local dentry key should be safe to reconcile after self-ack"
    );

    assert!(
        RemoteApplyCoordinator::new(&pg)
            .reconcile_serving_dentries_from_base_with_overlay(
                &HashSet::from([slot_key]),
                &overlay_after_ack,
            )
            .expect("reconcile deferred dentry"),
        "deferred base dentry should be replayed into serving"
    );
    assert_eq!(
        pg.lookup(ROOT_INO, b"slot.txt")
            .expect("remote slot visible after reconcile")
            .ino,
        remote_ino
    );
}

#[test]
fn foreign_unready_add_entry_does_not_hide_visible_local_regular_file() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let local_file = create_published_small_file(&pg, ROOT_INO, b"slot.txt", 77_018);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack published setup");

    let remote_ino = 77_019;
    let mut remote_attr = InodeAttr::new_file(remote_ino, ROOT_INO, 0, 0, 0o644);
    remote_attr.size = 227;
    remote_attr.content_epoch = 1;
    remote_attr.upload_state = UploadState::Updating;
    let remote_entry = DirEntryValue {
        ino: remote_ino,
        kind: FileType::RegularFile,
    };
    let remote_batch = [
        MetaEvent::SetAttr {
            attr: remote_attr.clone(),
        },
        MetaEvent::AddEntry {
            parent: ROOT_INO,
            name: b"slot.txt".to_vec(),
            entry: remote_entry,
        },
    ];

    let (_changed, protected_overlay) = apply_foreign_batch(
        &pg,
        &remote_batch,
        &OverlayConflictSets::default(),
        &HashSet::new(),
    )
    .expect("apply unready foreign add entry");
    assert!(
        protected_overlay
            .dentry_keys
            .contains(&PgMetaEngine::dentry_key(ROOT_INO, b"slot.txt")),
        "unready foreign regular-file dentry must be deferred until publish"
    );
    assert_eq!(
        pg.lookup(ROOT_INO, b"slot.txt")
            .expect("local slot remains visible")
            .ino,
        local_file.ino,
        "an unready foreign AddEntry must not hide the current serving source path"
    );
}

#[test]
fn foreign_base_publish_preserves_unflushed_local_overlay_payload() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"payload.txt", 77_020);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack published setup");

    set_file_state(&pg, file.ino, 64, 2);
    let local_payload = PayloadRef::new_partial(77_021, 64);
    pg.stage_payload_slice(file.ino, 0, &local_payload)
        .expect("stage local payload");
    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(overlay.block_inodes.contains(&file.ino));

    let remote_payload = PayloadRef::new_partial(77_022, 64);
    let mut remote_slice = SliceRecord::new(
        SliceId(0),
        SliceOrder(0),
        SliceRange::new(0, 64).expect("remote slice range"),
        remote_payload,
    );
    remote_slice.payload_state = PayloadRefState::Published;
    apply_foreign_batch(
        &pg,
        &[MetaEvent::PublishRegularFileBaseSlices {
            ino: file.ino,
            chunk_slices: vec![(ChunkId(0), vec![remote_slice])],
        }],
        &overlay,
        &HashSet::new(),
    )
    .expect("apply foreign publish");

    let object_ids: Vec<_> = pg
        .lookup_regular_file_slices(file.ino, 0, 64)
        .expect("lookup local payload")
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect();
    assert_eq!(
        object_ids,
        vec![local_payload.object_id],
        "unflushed local payload must remain visible over a conflicting remote publish"
    );
}

#[test]
fn decoded_replay_coalesces_regular_file_serving_reconciliation() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let first = create_published_small_file(&pg, ROOT_INO, b"first.bin", 77_023);
    let second = create_published_small_file(&pg, ROOT_INO, b"second.bin", 77_024);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack published setup");

    let mut first_remote = pg.get_attr(first.ino).expect("first attr");
    first_remote.size = 128;
    first_remote.committed_size = 128;
    first_remote.content_epoch = 2;
    first_remote.committed_content_epoch = 2;
    first_remote.mtime = first_remote.mtime.saturating_add(1);
    first_remote.ctime = first_remote.ctime.saturating_add(1);

    let mut second_remote = pg.get_attr(second.ino).expect("second attr");
    second_remote.size = 256;
    second_remote.committed_size = 256;
    second_remote.content_epoch = 2;
    second_remote.committed_content_epoch = 2;
    second_remote.mtime = second_remote.mtime.saturating_add(1);
    second_remote.ctime = second_remote.ctime.saturating_add(1);

    RedbMetaEngine::reset_regular_file_serving_reconcile_call_count_for_test();
    let changed = apply_decoded_foreign_batches_locked(
        &pg,
        vec![
            DecodedReplayBatch {
                batch_version: 1,
                events: vec![MetaEvent::SetAttr { attr: first_remote }],
                is_self_batch: false,
            },
            DecodedReplayBatch {
                batch_version: 2,
                events: vec![MetaEvent::SetAttr {
                    attr: second_remote,
                }],
                is_self_batch: false,
            },
        ],
        None,
        0,
    )
    .expect("apply decoded replay batches");

    assert!(changed, "replay should report applied metadata changes");
    assert_eq!(
        RedbMetaEngine::regular_file_serving_reconcile_call_count_for_test(),
        1,
        "regular-file serving reconciliation should be coalesced for the replay pass"
    );
    assert_eq!(pg.local_version.load(Ordering::SeqCst), 2);
    assert_eq!(pg.get_attr(first.ino).expect("first after").size, 128);
    assert_eq!(pg.get_attr(second.ino).expect("second after").size, 256);
}

#[test]
fn protected_attr_merge_accepts_newer_remote_shrink_epoch() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"protected-shrink.txt", 77_030);
    let mut current = pg.get_attr(file.ino).expect("load local attr");
    current.size = 2 * 1024 * 1024;
    current.committed_size = current.size;
    current.content_epoch = 5;
    current.committed_content_epoch = 5;
    current.upload_state = UploadState::Ready;
    pg.set_attr(&current).expect("seed local protected attr");

    let mut remote = current.clone();
    remote.size = 1024 * 1024;
    remote.committed_size = remote.size;
    remote.content_epoch = 6;
    remote.committed_content_epoch = 6;
    remote.mtime = remote.mtime.saturating_add(1);
    remote.ctime = remote.ctime.saturating_add(1);

    apply_foreign_batch(
        &pg,
        &[MetaEvent::SetAttr { attr: remote }],
        &OverlayConflictSets::default(),
        &HashSet::from([file.ino]),
    )
    .expect("apply newer remote shrink");

    let applied = pg.get_attr(file.ino).expect("load applied attr");
    assert_eq!(
        applied.size,
        1024 * 1024,
        "newer remote content epoch must be allowed to shrink a protected inode"
    );
    assert_eq!(applied.content_epoch, 6);
    assert_eq!(applied.committed_content_epoch, 6);
}

#[test]
fn protected_attr_merge_preserves_larger_size_for_newer_remote_payload_publish() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"protected-newer-write.txt", 77_035);
    let mut current = pg.get_attr(file.ino).expect("load local attr");
    current.size = 3_146_061;
    current.committed_size = current.size;
    current.content_epoch = 5;
    current.committed_content_epoch = 5;
    current.upload_state = UploadState::Ready;
    pg.set_attr(&current).expect("seed local protected attr");

    let mut remote = current.clone();
    remote.size = 3_145_926;
    remote.committed_size = remote.size;
    remote.content_epoch = 6;
    remote.committed_content_epoch = 6;
    remote.mtime = remote.mtime.saturating_add(1);
    remote.ctime = remote.ctime.saturating_add(1);

    let remote_tail = PayloadRef::new_partial(77_036, 26);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: vec![(
            ChunkId(0),
            vec![SliceRecord {
                slice_id: SliceId(0),
                order: SliceOrder(0),
                range: SliceRange::new(
                    3_145_900 % DEFAULT_CHUNK_SIZE,
                    (3_145_900 % DEFAULT_CHUNK_SIZE) + 26,
                )
                .expect("remote tail range"),
                payload: Some(remote_tail),
                payload_state: PayloadRefState::Published,
            }],
        )],
    };

    apply_foreign_batch(
        &pg,
        &[MetaEvent::SetAttr { attr: remote }, remote_publish],
        &OverlayConflictSets::default(),
        &HashSet::from([file.ino]),
    )
    .expect("apply newer remote write publish");

    let applied = pg.get_attr(file.ino).expect("load applied attr");
    assert_eq!(
        applied.size, current.size,
        "a newer remote write publish must not shrink a protected larger logical EOF"
    );
    assert_eq!(applied.content_epoch, 6);
    assert_eq!(applied.committed_content_epoch, 6);
}

#[test]
fn protected_foreign_publish_does_not_get_replaced_by_aux_replay() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"protected-aux-publish.txt", 77_040);
    let mut attr = pg.get_attr(file.ino).expect("load local attr");
    attr.content_epoch = 3;
    attr.committed_content_epoch = 3;
    attr.size = (3 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096;
    attr.committed_size = attr.size;
    attr.upload_state = UploadState::Ready;
    pg.set_attr(&attr).expect("seed local protected attr");

    let local_tail = PayloadRef::new_partial(77_041, 4096);
    pg.stage_payload_slice(file.ino, 3, &local_tail)
        .expect("stage local tail");
    mark_overlay_objects_uploaded(&pg, file.ino, 3, &[local_tail.object_id]);
    assert!(
        pg.local
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 3)
            .expect("publish local tail")
    );

    let remote_middle = PayloadRef::new_partial(77_042, 4096);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: fixture_payload_slices(2, Some(&remote_middle), PayloadRefState::Published)
            .expect("remote middle slices"),
    };
    apply_foreign_batch(
        &pg,
        &[remote_publish],
        &OverlayConflictSets::default(),
        &HashSet::from([file.ino]),
    )
    .expect("apply protected foreign publish");

    let object_ids: Vec<_> = pg
        .lookup_regular_file_slices(file.ino, 0, (4 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096)
        .expect("lookup merged protected base graph")
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect();
    assert_eq!(
        object_ids,
        vec![77_040, remote_middle.object_id, local_tail.object_id],
        "protected base merge must not be overwritten by aux replacement in the same foreign batch"
    );
}

#[test]
fn protected_foreign_publish_can_clear_committed_serving_overlay() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"protected-serving-overlay.txt", 77_050);
    let mut attr = pg.get_attr(file.ino).expect("load local attr");
    attr.size = 2 * u64::from(DEFAULT_BLOCK_SIZE);
    attr.committed_size = attr.size;
    attr.content_epoch = 3;
    attr.committed_content_epoch = 3;
    attr.upload_state = UploadState::Ready;
    pg.set_attr(&attr).expect("seed committed attr");
    pg.stage_payload_tombstone(file.ino, 1)
        .expect("stage stale serving tombstone");
    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(overlay.block_inodes.contains(&file.ino));

    let remote_tail = PayloadRef::new_partial(77_051, DEFAULT_BLOCK_SIZE);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: fixture_payload_slices(1, Some(&remote_tail), PayloadRefState::Published)
            .expect("remote tail slices"),
    };
    let (changed, _) =
        apply_foreign_batch(&pg, &[remote_publish], &overlay, &HashSet::from([file.ino]))
            .expect("apply protected foreign publish");
    assert!(
        changed,
        "protected publish-only batches that update serving metadata must wake VFS invalidation"
    );

    let object_ids: Vec<_> = pg
        .lookup_regular_file_slices(file.ino, 0, 2 * u64::from(DEFAULT_BLOCK_SIZE))
        .expect("lookup merged protected graph")
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect();
    assert_eq!(
        object_ids,
        vec![77_050, remote_tail.object_id],
        "a committed serving overlay must not keep hiding a protected base payload merge"
    );
}

#[test]
fn attr_only_foreign_batch_must_not_regress_published_file_content_state() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"attr-only-regression.txt", 77_052);
    let start = pg.flushed_event_seq.load(Ordering::SeqCst);
    let end = pg.local.get_counter("nextEventSeq").expect("event end");
    let local_events = pg
        .local
        .read_events_range(start, end)
        .expect("read local events");
    let projected_self_batch = RemoteApplyCoordinator::project_remote_events(&local_events);
    acknowledge_local_prefix_from_self_batch(&pg, &projected_self_batch)
        .expect("ack local seed publish");
    let mut winner_attr = pg.get_attr(file.ino).expect("load winner attr");
    winner_attr.size = u64::from(DEFAULT_BLOCK_SIZE);
    winner_attr.committed_size = u64::from(DEFAULT_BLOCK_SIZE);
    winner_attr.content_epoch = 3;
    winner_attr.committed_content_epoch = 3;
    winner_attr.upload_state = UploadState::Ready;
    let winner_payload = PayloadRef::new_full(77_053, DEFAULT_BLOCK_SIZE);
    let winner_batch = vec![
        MetaEvent::PublishRegularFileBaseSlices {
            ino: file.ino,
            chunk_slices: fixture_payload_slices(
                0,
                Some(&winner_payload),
                PayloadRefState::Published,
            )
            .expect("winner slices"),
        },
        MetaEvent::SetAttr {
            attr: winner_attr.clone(),
        },
    ];
    apply_foreign_batch(
        &pg,
        &winner_batch,
        &OverlayConflictSets::default(),
        &HashSet::new(),
    )
    .expect("apply winner batch");
    assert_eq!(
        lookup_object_ids(&pg, file.ino, winner_attr.size).expect("lookup winner object ids"),
        vec![winner_payload.object_id],
        "setup should expose the winner payload before the attr-only batch"
    );

    let mut stale_attr = winner_attr.clone();
    stale_attr.size = 0;
    stale_attr.committed_size = 0;
    stale_attr.content_epoch = 1;
    stale_attr.committed_content_epoch = 1;
    stale_attr.mtime = winner_attr.mtime.saturating_add(1);
    stale_attr.ctime = stale_attr.mtime;
    apply_foreign_batch(
        &pg,
        &[MetaEvent::SetAttr { attr: stale_attr }],
        &OverlayConflictSets::default(),
        &HashSet::new(),
    )
    .expect("apply attr-only stale batch");

    let attr = pg.get_attr(file.ino).expect("load final attr");
    assert_eq!(attr.size, u64::from(DEFAULT_BLOCK_SIZE));
    assert_eq!(attr.committed_size, u64::from(DEFAULT_BLOCK_SIZE));
    assert_eq!(attr.content_epoch, 3);
    assert_eq!(attr.committed_content_epoch, 3);
    assert_eq!(
        lookup_object_ids(&pg, file.ino, attr.size).expect("lookup final object ids"),
        vec![winner_payload.object_id],
        "an attr-only batch must not hide the latest published content graph"
    );
}

#[test]
fn attr_only_updating_foreign_batch_must_not_hide_published_file_content_state() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file =
        create_published_small_file(&pg, ROOT_INO, b"attr-only-updating-regression.txt", 77_054);
    let start = pg.flushed_event_seq.load(Ordering::SeqCst);
    let end = pg.local.get_counter("nextEventSeq").expect("event end");
    let local_events = pg
        .local
        .read_events_range(start, end)
        .expect("read local events");
    let projected_self_batch = RemoteApplyCoordinator::project_remote_events(&local_events);
    acknowledge_local_prefix_from_self_batch(&pg, &projected_self_batch)
        .expect("ack local seed publish");

    let mut winner_attr = pg.get_attr(file.ino).expect("load winner attr");
    winner_attr.size = u64::from(DEFAULT_BLOCK_SIZE);
    winner_attr.committed_size = u64::from(DEFAULT_BLOCK_SIZE);
    winner_attr.content_epoch = 5;
    winner_attr.committed_content_epoch = 5;
    winner_attr.upload_state = UploadState::Ready;
    let winner_payload = PayloadRef::new_full(77_055, DEFAULT_BLOCK_SIZE);
    let winner_batch = vec![
        MetaEvent::PublishRegularFileBaseSlices {
            ino: file.ino,
            chunk_slices: fixture_payload_slices(
                0,
                Some(&winner_payload),
                PayloadRefState::Published,
            )
            .expect("winner slices"),
        },
        MetaEvent::SetAttr {
            attr: winner_attr.clone(),
        },
    ];
    apply_foreign_batch(
        &pg,
        &winner_batch,
        &OverlayConflictSets::default(),
        &HashSet::new(),
    )
    .expect("apply winner batch");
    assert_eq!(
        lookup_object_ids(&pg, file.ino, winner_attr.size).expect("lookup winner object ids"),
        vec![winner_payload.object_id],
        "setup should expose the winner payload before the attr-only Updating batch"
    );
    let base_attr = pg
        .local
        .get_base_attr(file.ino)
        .expect("load base attr")
        .expect("base attr exists");
    assert_eq!(base_attr.size, u64::from(DEFAULT_BLOCK_SIZE));
    assert_eq!(base_attr.upload_state, UploadState::Ready);

    let mut stale_updating_attr = winner_attr.clone();
    stale_updating_attr.size = 0;
    stale_updating_attr.committed_size = 0;
    stale_updating_attr.content_epoch = 3;
    stale_updating_attr.committed_content_epoch = 2;
    stale_updating_attr.upload_state = UploadState::Updating;
    stale_updating_attr.mtime = winner_attr.mtime.saturating_add(1);
    stale_updating_attr.ctime = stale_updating_attr.mtime;
    apply_foreign_batch(
        &pg,
        &[MetaEvent::SetAttr {
            attr: stale_updating_attr,
        }],
        &OverlayConflictSets::default(),
        &HashSet::new(),
    )
    .expect("apply attr-only updating batch");

    let attr = pg.get_attr(file.ino).expect("load final attr");
    assert_eq!(attr.size, u64::from(DEFAULT_BLOCK_SIZE));
    assert_eq!(attr.committed_size, u64::from(DEFAULT_BLOCK_SIZE));
    assert_eq!(attr.content_epoch, 5);
    assert_eq!(attr.committed_content_epoch, 5);
    assert_eq!(attr.upload_state, UploadState::Ready);
    assert_eq!(
        lookup_object_ids(&pg, file.ino, attr.size).expect("lookup final object ids"),
        vec![winner_payload.object_id],
        "a late attr-only Updating batch must not hide the latest published content graph"
    );
}

#[test]
fn newer_attr_only_updating_foreign_batch_preserves_uploading_marker_and_old_content() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"newer-attr-only-updating.txt", 77_060);
    let start = pg.flushed_event_seq.load(Ordering::SeqCst);
    let end = pg.local.get_counter("nextEventSeq").expect("event end");
    let local_events = pg
        .local
        .read_events_range(start, end)
        .expect("read local events");
    let projected_self_batch = RemoteApplyCoordinator::project_remote_events(&local_events);
    acknowledge_local_prefix_from_self_batch(&pg, &projected_self_batch)
        .expect("ack local seed publish");

    let mut updating_attr = pg.get_attr(file.ino).expect("load attr");
    updating_attr.content_epoch = updating_attr.content_epoch.saturating_add(1);
    updating_attr.upload_state = UploadState::Updating;
    apply_foreign_batch(
        &pg,
        &[MetaEvent::SetAttr {
            attr: updating_attr.clone(),
        }],
        &OverlayConflictSets::default(),
        &HashSet::new(),
    )
    .expect("apply newer attr-only updating batch");

    let attr = pg.get_attr(file.ino).expect("load final attr");
    assert_eq!(attr.upload_state, UploadState::Updating);
    assert_eq!(attr.content_epoch, updating_attr.content_epoch);
    assert_eq!(
        attr.committed_content_epoch, updating_attr.committed_content_epoch,
        "the committed epoch must remain pinned to the old published content"
    );
    assert_eq!(
        lookup_object_ids(&pg, file.ino, attr.committed_size).expect("lookup object ids"),
        vec![77_060],
        "attr-only Updating must keep serving the previous committed payload"
    );
}

#[test]
fn sparse_ready_foreign_publish_preserves_disjoint_base_ranges_when_not_complete() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"sparse-ready-merge.txt", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, (4 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096, 3);
    let local_tail = PayloadRef::new_full(77_055, DEFAULT_BLOCK_SIZE);
    pg.stage_payload_slice(file.ino, 4, &local_tail)
        .expect("stage local tail");
    mark_overlay_objects_uploaded(&pg, file.ino, 3, &[local_tail.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 3)
            .expect("publish local tail"),
        "local sparse tail publish should succeed"
    );
    let start = pg.flushed_event_seq.load(Ordering::SeqCst);
    let end = pg.local.get_counter("nextEventSeq").expect("event end");
    let local_events = pg
        .local
        .read_events_range(start, end)
        .expect("read local events");
    let projected_self_batch = RemoteApplyCoordinator::project_remote_events(&local_events);
    acknowledge_local_prefix_from_self_batch(&pg, &projected_self_batch)
        .expect("ack local sparse publish");
    assert!(
        pg.local_overlay_conflicts()
            .expect("overlay conflicts")
            .block_inodes
            .is_empty(),
        "this reproduces an unprotected peer that already acked its own sparse publish"
    );
    assert_eq!(
        lookup_object_ids(&pg, file.ino, (4 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096)
            .expect("lookup local sparse graph"),
        vec![local_tail.object_id],
        "setup should have a committed sparse tail before the remote peer publishes"
    );

    let remote_middle = PayloadRef::new_full(77_056, DEFAULT_BLOCK_SIZE);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: fixture_payload_slices(2, Some(&remote_middle), PayloadRefState::Published)
            .expect("remote middle slices"),
    };
    let mut remote_attr = pg.get_attr(file.ino).expect("load attr");
    remote_attr.size = (4 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096;
    remote_attr.committed_size = remote_attr.size;
    remote_attr.content_epoch = 4;
    remote_attr.committed_content_epoch = 4;
    remote_attr.upload_state = UploadState::Ready;
    let remote_batch = vec![
        remote_publish,
        MetaEvent::SetAttr {
            attr: remote_attr.clone(),
        },
    ];
    assert!(
        RemoteApplyCoordinator::partial_regular_file_publish_inodes(&remote_batch)
            .contains(&file.ino),
        "sparse ready publish payloads do not cover committed EOF"
    );

    apply_foreign_batch(
        &pg,
        &remote_batch,
        &OverlayConflictSets::default(),
        &HashSet::new(),
    )
    .expect("apply sparse ready foreign publish");

    let object_ids = pg
        .lookup_regular_file_slices(file.ino, 0, remote_attr.size)
        .expect("lookup merged sparse graph")
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect::<Vec<_>>();
    assert_eq!(
        object_ids,
        vec![remote_middle.object_id, local_tail.object_id],
        "a sparse ready publish that does not cover committed EOF must not erase disjoint published ranges"
    );
}

#[test]
fn sparse_ready_self_batch_preserves_imported_disjoint_base_ranges_when_not_complete() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"sparse-ready-self-merge.txt", 0, 0, 0o644)
        .expect("create file");
    let logical_size = (4 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096;
    set_file_state(&pg, file.ino, logical_size, 3);

    let imported_tail = PayloadRef::new_full(77_057, DEFAULT_BLOCK_SIZE);
    let imported_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: fixture_payload_slices(4, Some(&imported_tail), PayloadRefState::Published)
            .expect("imported tail slices"),
    };
    pg.local
        .apply_replica_events_to_base_preserving_protected_base_ranges(
            std::slice::from_ref(&imported_publish),
            &HashSet::from([file.ino]),
        )
        .expect("seed imported sparse tail");
    assert_eq!(
        lookup_object_ids(&pg, file.ino, logical_size).expect("lookup imported graph"),
        vec![imported_tail.object_id],
        "setup should have an imported sparse tail before self replay"
    );

    let local_middle = PayloadRef::new_full(77_058, DEFAULT_BLOCK_SIZE);
    let local_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: fixture_payload_slices(2, Some(&local_middle), PayloadRefState::Published)
            .expect("local middle slices"),
    };
    let mut local_attr = pg.get_attr(file.ino).expect("load attr");
    local_attr.size = logical_size;
    local_attr.committed_size = local_attr.size;
    local_attr.content_epoch = 4;
    local_attr.committed_content_epoch = 4;
    local_attr.upload_state = UploadState::Ready;
    let self_batch = vec![
        local_publish,
        MetaEvent::SetAttr {
            attr: local_attr.clone(),
        },
    ];
    assert!(
        RemoteApplyCoordinator::partial_regular_file_publish_inodes(&self_batch)
            .contains(&file.ino),
        "self sparse publish payloads do not cover committed EOF"
    );

    apply_visible_self_batch(&pg, &self_batch).expect("apply visible self sparse publish");

    let object_ids =
        lookup_object_ids(&pg, file.ino, logical_size).expect("lookup merged sparse graph");
    assert_eq!(
        object_ids,
        vec![local_middle.object_id, imported_tail.object_id],
        "a sparse self publish that does not cover committed EOF must not erase imported disjoint ranges"
    );
}

#[test]
fn lifecycle_publish_ready_without_overlay_still_protects_foreign_replay() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(
            ROOT_INO,
            b"publish-ready-lifecycle-protect.txt",
            0,
            0,
            0o644,
        )
        .expect("create file");
    let mut attr = pg.get_attr(file.ino).expect("load attr");
    attr.size = 4096;
    attr.content_epoch = attr.content_epoch.saturating_add(1);
    pg.set_attr(&attr)
        .expect("stage blockless publish-ready attr");

    assert!(
        !pg.local
            .has_local_regular_file_overlay(file.ino)
            .expect("probe regular-file overlay"),
        "setup should rely on lifecycle state, not chunk overlay rows"
    );
    let lifecycle = pg
        .local
        .get_inode_lifecycle(file.ino)
        .expect("load lifecycle")
        .expect("lifecycle should exist");
    assert_eq!(lifecycle.state, InodeLifecycleState::PublishReady);

    assert!(
        RemoteApplyCoordinator::new(&pg)
            .local_regular_file_state_needs_foreign_protection(file.ino)
            .expect("probe protection"),
        "publish-ready lifecycle rows represent local publication work even without chunk overlay rows"
    );
}

#[test]
fn stale_local_only_lifecycle_without_overlay_does_not_protect_committed_base_attr() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"stale-lifecycle-ready.txt", 87);
    let published_attr = pg.get_attr(file.ino).expect("load published attr");

    let mut local_attr = published_attr.clone();
    local_attr.size = local_attr.size.saturating_add(128);
    local_attr.content_epoch = local_attr.content_epoch.saturating_add(1);
    local_attr.upload_state = UploadState::Updating;
    pg.set_attr(&local_attr)
        .expect("stage local attr lifecycle");

    assert_ne!(
        pg.local
            .get_inode_lifecycle(file.ino)
            .expect("load local lifecycle")
            .expect("local lifecycle")
            .state,
        InodeLifecycleState::Published
    );

    pg.local
        .apply_replica_events_to_base(&[MetaEvent::SetAttr {
            attr: published_attr.clone(),
        }])
        .expect("restore committed base attr without refreshing stale lifecycle");
    pg.local
        .apply_replica_serving_state(&[MetaEvent::SetAttr {
            attr: published_attr.clone(),
        }])
        .expect("restore visible attr without refreshing stale lifecycle");

    assert!(
        !pg.local
            .has_local_regular_file_overlay(file.ino)
            .expect("probe overlay"),
        "setup must not have local overlay rows"
    );
    assert_ne!(
        pg.local
            .get_inode_lifecycle(file.ino)
            .expect("load stale lifecycle")
            .expect("stale lifecycle")
            .state,
        InodeLifecycleState::Published,
        "setup should retain a stale unpublished lifecycle row like mounted replay"
    );
    let current_attr = pg.local.get_attr(file.ino).expect("load current attr");
    assert_eq!(current_attr.upload_state, UploadState::Ready);
    assert_eq!(current_attr.size, current_attr.committed_size);
    assert_eq!(
        current_attr.content_epoch,
        current_attr.committed_content_epoch
    );
    assert!(
        !RemoteApplyCoordinator::new(&pg)
            .local_regular_file_state_needs_foreign_protection(file.ino)
            .expect("probe protection"),
        "a stale LocalOnly lifecycle row must not protect a Ready committed base attr without overlay"
    );
}

#[test]
fn attr_only_lifecycle_overlay_does_not_block_committed_base_payload_replay() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = create_published_small_file(&pg, ROOT_INO, b"attr-only-block-replay.txt", 87);

    let mut local_attr = pg.get_attr(file.ino).expect("load local attr");
    local_attr.size = 8192;
    pg.set_attr(&local_attr).expect("set local attr overlay");

    assert!(
        RemoteApplyCoordinator::new(&pg)
            .local_regular_file_state_needs_foreign_protection(file.ino)
            .expect("probe attr protection"),
        "local attr lifecycle must still protect the local attr overlay"
    );
    assert!(
        !pg.local
            .has_local_regular_file_overlay(file.ino)
            .expect("probe block overlay"),
        "setup must not have local chunk overlay rows"
    );

    let overlay = RemoteApplyCoordinator::new(&pg)
        .current_regular_file_overlay_conflicts_for_batch(&[MetaEvent::SetAttr {
            attr: local_attr.clone(),
        }])
        .expect("collect overlay conflicts");
    assert!(overlay.attr_inodes.contains(&file.ino));
    assert!(
        !overlay.block_inodes.contains(&file.ino),
        "attr-only lifecycle protection must not protect committed base ranges"
    );

    let remote_payload = PayloadRef::new_full(88, DEFAULT_BLOCK_SIZE);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: fixture_payload_slices(0, Some(&remote_payload), PayloadRefState::Published)
            .expect("remote payload slices"),
    };
    let mut remote_attr = pg.get_attr(file.ino).expect("load remote attr");
    remote_attr.size = 4096;
    remote_attr.committed_size = 4096;
    remote_attr.content_epoch = remote_attr.content_epoch.saturating_add(1);
    remote_attr.committed_content_epoch = remote_attr.content_epoch;
    remote_attr.upload_state = UploadState::Ready;

    apply_foreign_batch(
        &pg,
        &[
            MetaEvent::SetAttr {
                attr: remote_attr.clone(),
            },
            remote_publish,
        ],
        &overlay,
        &HashSet::new(),
    )
    .expect("apply committed payload under local attr overlay");

    assert_eq!(
        lookup_first_object_id(&pg, file.ino).expect("lookup committed payload"),
        Some(remote_payload.object_id),
        "committed payload replay must replace the base graph despite local attr overlay"
    );
    assert_eq!(
        pg.get_attr(file.ino)
            .expect("local attr should remain")
            .size,
        8192,
        "local attr overlay should still mask remote size"
    );
}

#[test]
fn newer_ready_foreign_publish_replaces_protected_committed_base_graph() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"protected-ready-replace.txt", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 233, 2);
    let local_head = PayloadRef::new_partial(77_060, 233);
    pg.stage_payload_slice(file.ino, 0, &local_head)
        .expect("stage local head");
    mark_overlay_objects_uploaded(&pg, file.ino, 2, &[local_head.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 2)
            .expect("publish local head"),
        "local head publish should succeed"
    );

    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(
        overlay.block_inodes.contains(&file.ino),
        "unacknowledged local publish should protect the inode before the remote winner arrives"
    );

    let remote_head = PayloadRef::new_partial(77_061, 229);
    let remote_tail = PayloadRef::new_partial(77_062, 229);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: vec![(
            ChunkId(0),
            vec![
                SliceRecord {
                    slice_id: SliceId(0),
                    order: SliceOrder(0),
                    range: SliceRange::new(0, 229).expect("head range"),
                    payload: Some(remote_head.clone()),
                    payload_state: PayloadRefState::Published,
                },
                SliceRecord {
                    slice_id: SliceId(1),
                    order: SliceOrder(1),
                    range: SliceRange::new(229, 458).expect("tail range"),
                    payload: Some(remote_tail.clone()),
                    payload_state: PayloadRefState::Published,
                },
            ],
        )],
    };
    let mut remote_attr = pg.get_attr(file.ino).expect("local attr");
    remote_attr.size = 458;
    remote_attr.committed_size = 458;
    remote_attr.content_epoch = 2;
    remote_attr.committed_content_epoch = 2;
    remote_attr.upload_state = UploadState::Ready;
    remote_attr.mtime = remote_attr.mtime.saturating_add(1);
    remote_attr.ctime = remote_attr.ctime.saturating_add(1);

    apply_foreign_batch(
        &pg,
        &[
            remote_publish,
            MetaEvent::SetAttr {
                attr: remote_attr.clone(),
            },
        ],
        &overlay,
        &HashSet::from([file.ino]),
    )
    .expect("apply newer ready foreign publish");

    let spans = pg
        .lookup_regular_file_slices(file.ino, 0, remote_attr.size)
        .expect("lookup merged graph");
    let object_ids = spans
        .iter()
        .filter_map(|span| span.payload.as_ref().map(|payload| payload.object_id))
        .collect::<Vec<_>>();
    assert_eq!(
        object_ids,
        vec![remote_head.object_id, remote_tail.object_id],
        "newer ready remote publish should replace, not splice into, a protected committed base graph"
    );
}

#[test]
fn growing_ready_foreign_publish_replaces_higher_epoch_protected_base_graph() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"protected-ready-grow.txt", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 235, 1);
    let local_head = PayloadRef::new_partial(77_070, 235);
    pg.stage_payload_slice(file.ino, 0, &local_head)
        .expect("stage local head");
    mark_overlay_objects_uploaded(&pg, file.ino, 1, &[local_head.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish local head"),
        "local head publish should succeed"
    );

    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(
        overlay.block_inodes.contains(&file.ino),
        "unacknowledged local publish should protect the inode before the remote growth arrives"
    );

    let remote_head = PayloadRef::new_partial(77_071, 231);
    let remote_tail = PayloadRef::new_partial(77_072, 231);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: vec![(
            ChunkId(0),
            vec![
                SliceRecord {
                    slice_id: SliceId(0),
                    order: SliceOrder(0),
                    range: SliceRange::new(0, 231).expect("head range"),
                    payload: Some(remote_head.clone()),
                    payload_state: PayloadRefState::Published,
                },
                SliceRecord {
                    slice_id: SliceId(1),
                    order: SliceOrder(1),
                    range: SliceRange::new(231, 462).expect("tail range"),
                    payload: Some(remote_tail.clone()),
                    payload_state: PayloadRefState::Published,
                },
            ],
        )],
    };
    let mut remote_attr = pg.get_attr(file.ino).expect("local attr");
    remote_attr.size = 462;
    remote_attr.committed_size = 462;
    remote_attr.content_epoch = 0;
    remote_attr.committed_content_epoch = 0;
    remote_attr.upload_state = UploadState::Ready;

    apply_foreign_batch(
        &pg,
        &[
            remote_publish,
            MetaEvent::SetAttr {
                attr: remote_attr.clone(),
            },
        ],
        &overlay,
        &HashSet::from([file.ino]),
    )
    .expect("apply growing ready foreign publish");

    let object_ids = pg
        .lookup_regular_file_slices(file.ino, 0, remote_attr.size)
        .expect("lookup merged graph")
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect::<Vec<_>>();
    assert_eq!(
        object_ids,
        vec![remote_head.object_id, remote_tail.object_id],
        "growing ready remote publish should replace, not splice into, a protected committed base graph"
    );
    assert_eq!(
        pg.get_attr(file.ino).expect("load applied attr").size,
        remote_attr.size,
        "serving attr must grow with the authoritative ready foreign publish"
    );
}

#[test]
fn ready_foreign_overwrite_replaces_larger_same_epoch_protected_base_graph() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"protected-ready-overwrite.txt", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 223, 0);
    let local_head = PayloadRef::new_partial(77_090, 223);
    pg.stage_payload_slice(file.ino, 0, &local_head)
        .expect("stage local head");
    mark_overlay_objects_uploaded(&pg, file.ino, 0, &[local_head.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 0)
            .expect("publish local head"),
        "local head publish should succeed"
    );

    set_file_state(&pg, file.ino, 446, 1);
    let local_tail = PayloadRef::new_partial(77_091, 223);
    pg.stage_payload_slice(file.ino, 223, &local_tail)
        .expect("stage local tail");
    mark_overlay_objects_uploaded(&pg, file.ino, 1, &[local_tail.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish local tail"),
        "local append publish should succeed"
    );

    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(
        overlay.block_inodes.contains(&file.ino),
        "unacknowledged local append should protect the inode before the remote overwrite arrives"
    );

    let remote_payload = PayloadRef::new_partial(77_092, 227);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: vec![(
            ChunkId(0),
            vec![SliceRecord {
                slice_id: SliceId(0),
                order: SliceOrder(0),
                range: SliceRange::new(0, 227).expect("remote overwrite range"),
                payload: Some(remote_payload.clone()),
                payload_state: PayloadRefState::Published,
            }],
        )],
    };
    let mut remote_attr = pg.get_attr(file.ino).expect("local attr");
    remote_attr.size = 227;
    remote_attr.committed_size = 227;
    remote_attr.content_epoch = 1;
    remote_attr.committed_content_epoch = 1;
    remote_attr.upload_state = UploadState::Ready;
    remote_attr.mtime = remote_attr.mtime.saturating_sub(1);
    remote_attr.ctime = remote_attr.ctime.saturating_sub(1);

    apply_foreign_batch(
        &pg,
        &[
            remote_publish,
            MetaEvent::SetAttr {
                attr: remote_attr.clone(),
            },
        ],
        &overlay,
        &HashSet::from([file.ino]),
    )
    .expect("apply ready foreign overwrite");

    let object_ids = pg
        .lookup_regular_file_slices(file.ino, 0, remote_attr.size)
        .expect("lookup merged graph")
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect::<Vec<_>>();
    assert_eq!(
        object_ids,
        vec![remote_payload.object_id],
        "ready remote overwrite should replace, not splice into, a larger same-epoch protected graph"
    );
}

#[test]
fn publish_without_ready_attr_preserves_protected_base_graph() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"protected-publish-only.txt", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 235, 1);
    let local_head = PayloadRef::new_partial(77_080, 235);
    pg.stage_payload_slice(file.ino, 0, &local_head)
        .expect("stage local head");
    mark_overlay_objects_uploaded(&pg, file.ino, 1, &[local_head.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish local head"),
        "local head publish should succeed"
    );

    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    let remote_head = PayloadRef::new_partial(77_081, 231);
    let remote_tail = PayloadRef::new_partial(77_082, 231);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: vec![(
            ChunkId(0),
            vec![
                SliceRecord {
                    slice_id: SliceId(0),
                    order: SliceOrder(0),
                    range: SliceRange::new(0, 231).expect("head range"),
                    payload: Some(remote_head),
                    payload_state: PayloadRefState::Published,
                },
                SliceRecord {
                    slice_id: SliceId(1),
                    order: SliceOrder(1),
                    range: SliceRange::new(231, 462).expect("tail range"),
                    payload: Some(remote_tail.clone()),
                    payload_state: PayloadRefState::Published,
                },
            ],
        )],
    };

    apply_foreign_batch(&pg, &[remote_publish], &overlay, &HashSet::from([file.ino]))
        .expect("apply publish-only foreign batch");

    let object_ids = pg
        .lookup_regular_file_slices(file.ino, 0, 462)
        .expect("lookup merged graph")
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect::<Vec<_>>();
    assert_eq!(
        object_ids,
        vec![local_head.object_id, remote_tail.object_id],
        "publish-only foreign batches are not authoritative enough to replace protected base ranges"
    );
}

#[test]
fn ready_complete_publish_preserved_base_ranges_sets_remote_serving_size() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"protected-ready-preserved-size.txt", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 446, 4);
    let local_payload = PayloadRef::new_partial(77_083, 446);
    pg.stage_payload_slice(file.ino, 0, &local_payload)
        .expect("stage local payload");
    mark_overlay_objects_uploaded(&pg, file.ino, 4, &[local_payload.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 4)
            .expect("publish local payload"),
        "local payload publish should succeed"
    );

    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    let remote_full = PayloadRef::new_partial(77_084, 227);
    let remote_publish = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: vec![(
            ChunkId(0),
            vec![SliceRecord {
                slice_id: SliceId(0),
                order: SliceOrder(0),
                range: SliceRange::new(0, 227).expect("remote full range"),
                payload: Some(remote_full),
                payload_state: PayloadRefState::Published,
            }],
        )],
    };
    let mut ready_attr = pg.get_attr(file.ino).expect("load current attr");
    ready_attr.size = 227;
    ready_attr.committed_size = 227;
    ready_attr.content_epoch = 4;
    ready_attr.committed_content_epoch = 4;
    ready_attr.upload_state = UploadState::Ready;

    apply_foreign_batch(
        &pg,
        &[
            remote_publish,
            MetaEvent::SetAttr {
                attr: ready_attr.clone(),
            },
        ],
        &overlay,
        &HashSet::from([file.ino]),
    )
    .expect("apply ready foreign publish");

    let served = pg.get_attr(file.ino).expect("load served attr");
    assert_eq!(
        served.size, ready_attr.size,
        "full Ready foreign publish must set remote serving size even when protected base ranges are preserved"
    );
}

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

        tx.execute("SELECT pg_advisory_xact_lock($1)", &[&902_202_603_i64])
            .await
            .map_err(|e| {
                MetaError::Storage(format!("postgres advisory lock failed: {}", pg_err(e)))
            })?;

        tx.batch_execute(
            r#"
                CREATE TABLE IF NOT EXISTS volumes (
                    vol BIGSERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    uuid TEXT NOT NULL UNIQUE,
                    block_size INTEGER NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                CREATE TABLE IF NOT EXISTS event_log (
                    vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
                    seq_id BIGSERIAL PRIMARY KEY,
                    batch_version BIGINT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_payload BYTEA NOT NULL,
                    client_id TEXT,
                    client_timestamp BIGINT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                CREATE TABLE IF NOT EXISTS metadata_state (
                    vol BIGINT PRIMARY KEY REFERENCES volumes(vol) ON DELETE CASCADE,
                    version BIGINT NOT NULL,
                    snapshot BYTEA NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                CREATE TABLE IF NOT EXISTS acl_rules (
                    vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
                    ino BIGINT NOT NULL,
                    principal TEXT NOT NULL,
                    access_level SMALLINT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (vol, ino, principal)
                );
                CREATE TABLE IF NOT EXISTS snapshots (
                    vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
                    id TEXT NOT NULL,
                    root_ino BIGINT NOT NULL,
                    created_at BIGINT NOT NULL,
                    state SMALLINT NOT NULL,
                    pinned_object_ids BIGINT[] NOT NULL DEFAULT '{}',
                    snapshot_payload BYTEA NOT NULL DEFAULT ''::bytea,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (vol, id)
                );
                CREATE TABLE IF NOT EXISTS file_lock_leases (
                    vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
                    ino BIGINT NOT NULL,
                    kind SMALLINT NOT NULL,
                    exclusive_owner TEXT,
                    shared_owners TEXT[] NOT NULL DEFAULT '{}',
                    lease_expires_at_unix_ms BIGINT NOT NULL,
                    generation BIGINT NOT NULL DEFAULT 1,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (vol, ino)
                );
                ALTER TABLE event_log
                    ADD COLUMN IF NOT EXISTS batch_version BIGINT;
                UPDATE event_log
                    SET batch_version = 0
                    WHERE batch_version IS NULL;
                ALTER TABLE event_log
                    ALTER COLUMN batch_version SET NOT NULL;
                "#,
        )
        .await
        .map_err(|e| MetaError::Storage(format!("postgres schema ensure failed: {}", pg_err(e))))?;

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

        let empty_snapshot = serde_json::to_vec(&RedbSnapshot::default())
            .map_err(|e| MetaError::Serialization(format!("snapshot encode failed: {e}")))?;
        tx.execute(
            r#"
                INSERT INTO metadata_state (vol, version, snapshot)
                VALUES ($1, 0, $2)
                ON CONFLICT (vol) DO NOTHING
                "#,
            &[&volume_id, &empty_snapshot],
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
        block_size: DEFAULT_BLOCK_SIZE,
    }
}

#[test]
fn test_local_overlay_conflicts_reserve_pending_xattr_and_symlink() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .local
        .create_file(ROOT_INO, b"file.txt", 0, 0, 0o644)
        .expect("create file");
    let sym = pg
        .local
        .create_symlink(ROOT_INO, b"link.txt", b"target-a", 0, 0)
        .expect("create symlink");

    let baseline = pg
        .local
        .get_counter("nextEventSeq")
        .expect("read baseline seq");
    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        *state = PendingReplicationState::default();
    }

    pg.set_xattr(file.ino, b"user.color", b"red", 0)
        .expect("set local xattr");
    pg.set_symlink_target(sym.ino, b"target-b")
        .expect("set local symlink");

    let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
    assert!(
        !overlay.block_inodes.contains(&file.ino),
        "xattr reservation should not masquerade as block overlay"
    );
    assert!(overlay.symlink_inodes.contains(&sym.ino));
    assert!(overlay.xattr_inodes.contains(&file.ino));
    assert!(
        overlay
            .xattr_keys
            .contains(&PgMetaEngine::xattr_key(file.ino, b"user.color"))
    );
    assert!(
        !overlay
            .xattr_keys
            .contains(&PgMetaEngine::xattr_key(file.ino, b"user.other"))
    );
    assert!(RemoteApplyCoordinator::aux_event_conflicts_with_overlay(
        &MetaEvent::SetXattr {
            ino: file.ino,
            name: b"user.color".to_vec(),
            value: b"blue".to_vec(),
            flags: 0,
        },
        &overlay,
        &HashSet::new(),
        &HashSet::new(),
    ));
    assert!(RemoteApplyCoordinator::aux_event_conflicts_with_overlay(
        &MetaEvent::SetSymlinkTarget {
            ino: sym.ino,
            target: b"target-c".to_vec(),
        },
        &overlay,
        &HashSet::new(),
        &HashSet::new(),
    ));

    let state = pg.pending_state.lock().expect("pending_state");
    let start = state.event_start.expect("pending start set");
    let end = state.event_end;
    assert_eq!(start, baseline);
    assert!(end > start);
}

#[test]
fn startup_snapshot_is_skipped_when_local_overlay_exists() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.local
        .create_file(ROOT_INO, b"draft.txt", 0, 0, 0o644)
        .expect("create local draft");

    assert!(
        PgMetaEngine::has_any_local_overlay_mutation(&pg.local).expect("local overlay mutation"),
        "local draft namespace should register as a local overlay mutation"
    );
    assert!(
        !PgMetaEngine::should_pull_startup_snapshot(&pg.local, 0)
            .expect("startup snapshot decision"),
        "startup snapshot must not clobber a local overlay after crash/restart"
    );
}

#[test]
fn persisted_pg_version_roundtrips_in_local_counter() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.persist_local_version(42).expect("persist local version");

    assert_eq!(pg.local_version.load(Ordering::SeqCst), 42);
    assert_eq!(
        PgMetaEngine::load_persisted_local_version(&pg.local).expect("reload local version"),
        42
    );
    assert!(
        !PgMetaEngine::should_pull_startup_snapshot(&pg.local, 42)
            .expect("startup snapshot decision"),
        "persisted PG version should suppress checkpoint bootstrap overwrite"
    );
}

#[test]
fn startup_snapshot_replay_starts_from_snapshot_epoch_not_pg_head() {
    let mut snapshot = RedbSnapshot::default();
    snapshot
        .counters
        .push((LOCAL_PG_VERSION_COUNTER.to_string(), 3));

    assert_eq!(
        PgMetaEngine::snapshot_replay_start_version(&snapshot, 9, 0),
        3,
        "metadata_state.version is the remote head; snapshot import must resume from the checkpoint epoch"
    );
}

#[test]
fn startup_snapshot_replay_caps_snapshot_epoch_to_pg_head() {
    let mut snapshot = RedbSnapshot::default();
    snapshot
        .counters
        .push((LOCAL_PG_VERSION_COUNTER.to_string(), 12));

    assert_eq!(
        PgMetaEngine::snapshot_replay_start_version(&snapshot, 9, 0),
        9,
        "a stale or corrupt checkpoint cursor must not advance past the advertised PG head"
    );
}

#[test]
fn startup_snapshot_replay_uses_previous_epoch_when_snapshot_has_no_cursor() {
    let snapshot = RedbSnapshot::default();

    assert_eq!(
        PgMetaEngine::snapshot_replay_start_version(&snapshot, 9, 2),
        2,
        "legacy checkpoints without a PG cursor must keep the previous local replay floor"
    );
}

#[test]
fn persisted_pg_client_slot_roundtrips_in_local_counter() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");

    assert_eq!(
        PgMetaEngine::load_persisted_client_slot(&local).expect("initial client slot"),
        None
    );
    PgMetaEngine::persist_client_slot(&local, 7).expect("persist client slot");
    assert_eq!(
        PgMetaEngine::load_persisted_client_slot(&local).expect("reload client slot"),
        Some(7)
    );
}

#[test]
fn remote_snapshot_does_not_export_local_pg_state() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    PgMetaEngine::persist_client_slot(&local, 7).expect("persist client slot");
    local
        .write_counters(|counters| {
            counters.set(LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER, 150)?;
            counters.set(LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER, 1_000)?;
            counters.set(LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER, 1_100)?;
            counters.set(LEGACY_LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER, 1_200)?;
            counters.set(LOCAL_PG_INODE_CURRENT_WINDOW_START_COUNTER, 2_000)?;
            counters.set(LOCAL_PG_INODE_CURRENT_WINDOW_END_COUNTER, 2_100)?;
            counters.set(LOCAL_PG_INODE_PREFETCHED_WINDOW_START_COUNTER, 2_100)?;
            counters.set(LOCAL_PG_INODE_PREFETCHED_WINDOW_NEXT_COUNTER, 2_100)?;
            counters.set(LOCAL_PG_INODE_PREFETCHED_WINDOW_END_COUNTER, 2_200)?;
            Ok(())
        })
        .expect("seed local pg counters");

    let snapshot = local
        .export_remote_snapshot()
        .expect("export remote snapshot");

    for local_counter in [
        LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER,
        LOCAL_PG_CLIENT_SLOT_COUNTER,
        LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER,
        LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER,
        LEGACY_LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER,
        LOCAL_PG_INODE_CURRENT_WINDOW_START_COUNTER,
        LOCAL_PG_INODE_CURRENT_WINDOW_END_COUNTER,
        LOCAL_PG_INODE_PREFETCHED_WINDOW_START_COUNTER,
        LOCAL_PG_INODE_PREFETCHED_WINDOW_NEXT_COUNTER,
        LOCAL_PG_INODE_PREFETCHED_WINDOW_END_COUNTER,
    ] {
        assert!(
            snapshot
                .counters
                .iter()
                .all(|(name, _)| name != local_counter),
            "remote discovery snapshots must not export local-only PG counter {local_counter}"
        );
    }
}

#[test]
fn remote_snapshot_import_drops_legacy_local_pg_state() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    PgMetaEngine::persist_client_slot(&local, 3).expect("persist reserved local slot");

    let mut legacy_snapshot = RedbSnapshot::default();
    legacy_snapshot.counters.extend([
        (LOCAL_PG_VERSION_COUNTER.to_string(), 9),
        (LOCAL_PG_NAMESPACE_VERSION_COUNTER.to_string(), 8),
        (LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER.to_string(), 150),
        (LOCAL_PG_CLIENT_SLOT_COUNTER.to_string(), 8),
        (
            LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER.to_string(),
            1_000,
        ),
        (
            LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER.to_string(),
            1_100,
        ),
        (
            LEGACY_LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER.to_string(),
            1_200,
        ),
        (
            LOCAL_PG_INODE_CURRENT_WINDOW_START_COUNTER.to_string(),
            2_000,
        ),
        (LOCAL_PG_INODE_CURRENT_WINDOW_END_COUNTER.to_string(), 2_100),
        (
            LOCAL_PG_INODE_PREFETCHED_WINDOW_START_COUNTER.to_string(),
            2_100,
        ),
        (
            LOCAL_PG_INODE_PREFETCHED_WINDOW_NEXT_COUNTER.to_string(),
            2_100,
        ),
        (
            LOCAL_PG_INODE_PREFETCHED_WINDOW_END_COUNTER.to_string(),
            2_200,
        ),
    ]);

    local
        .import_remote_snapshot(&legacy_snapshot)
        .expect("import legacy remote snapshot");

    assert_eq!(
        PgMetaEngine::load_persisted_client_slot(&local).expect("load client slot"),
        None,
        "remote snapshot import must not reuse the checkpoint writer slot"
    );
    assert_eq!(
        local
            .get_counter(LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER)
            .expect("load flushed cursor"),
        0,
        "remote snapshot import must drop legacy local event-log cursor"
    );
    assert_eq!(
        local
            .get_counter(LOCAL_PG_VERSION_COUNTER)
            .expect("load applied version"),
        9,
        "remote replay cursor must remain part of the imported data snapshot"
    );
}

#[test]
fn snapshot_reconcile_resets_stale_regular_file_serving_attr() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"snapshot-stale-serving.txt", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 227, 1);
    let local_payload = PayloadRef::new_partial(91_001, 227);
    pg.stage_payload_slice(file.ino, 0, &local_payload)
        .expect("stage local payload");
    mark_overlay_objects_uploaded(&pg, file.ino, 1, &[local_payload.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish local payload"),
        "local publish should make the file unprotected"
    );
    let event_end = pg.local.get_counter("nextEventSeq").expect("event end");
    pg.flushed_event_seq.store(event_end, Ordering::SeqCst);

    let mut remote_attr = pg.get_attr(file.ino).expect("load published attr");
    remote_attr.size = 223;
    remote_attr.committed_size = 223;
    remote_attr.content_epoch = 1;
    remote_attr.committed_content_epoch = 1;
    remote_attr.upload_state = UploadState::Ready;
    pg.local
        .apply_replica_events_to_base(&[MetaEvent::SetAttr {
            attr: remote_attr.clone(),
        }])
        .expect("apply remote base attr");

    assert_eq!(
        pg.get_attr(file.ino).expect("load stale serving attr").size,
        227,
        "base-only import can leave a stale serving attr until reconciliation"
    );

    pg.reconcile_imported_snapshot_regular_files()
        .expect("reconcile imported snapshot");

    assert_eq!(
        pg.get_attr(file.ino).expect("load reconciled attr").size,
        remote_attr.size,
        "snapshot reconciliation must reset stale regular-file serving attrs from base"
    );
}

#[test]
fn recent_flush_reconcile_resets_stale_regular_file_serving_attr() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"recent-flush-stale-serving.txt", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 227, 3);
    let local_payload = PayloadRef::new_partial(91_011, 227);
    pg.stage_payload_slice(file.ino, 0, &local_payload)
        .expect("stage local payload");
    mark_overlay_objects_uploaded(&pg, file.ino, 3, &[local_payload.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 3)
            .expect("publish local payload"),
        "local publish should succeed"
    );
    let events_start = pg.flushed_event_seq.load(Ordering::SeqCst);
    let events_end = pg.local.get_counter("nextEventSeq").expect("events end");
    let local_events = pg
        .local
        .read_events_range(events_start, events_end)
        .expect("read local events");
    pg.persist_flushed_event_seq(events_end)
        .expect("mark local publish flushed");

    let mut remote_attr = pg.get_attr(file.ino).expect("load published attr");
    remote_attr.size = 446;
    remote_attr.committed_size = 446;
    remote_attr.content_epoch = 4;
    remote_attr.committed_content_epoch = 4;
    remote_attr.upload_state = UploadState::Ready;
    pg.local
        .apply_replica_events_to_base(&[MetaEvent::SetAttr {
            attr: remote_attr.clone(),
        }])
        .expect("apply authoritative base attr");
    pg.persist_local_version(7).expect("catch up local version");

    assert_eq!(
        pg.get_attr(file.ino).expect("load stale serving attr").size,
        227,
        "flushed local serving attr can remain stale after base advances"
    );

    let recent_flush = RecentFlush {
        batch_version: 7,
        overlay: RemoteApplyCoordinator::overlay_conflicts_for_events(
            &RemoteApplyCoordinator::project_remote_events(&local_events),
        ),
    };
    reconcile_recent_flush_regular_files(&pg, Some(&recent_flush)).expect("reconcile recent flush");

    assert_eq!(
        pg.get_attr(file.ino).expect("load reconciled attr").size,
        remote_attr.size,
        "recent-flush reconciliation must reset stale regular-file serving attrs from base"
    );
}

fn persist_inode_state(local: &RedbMetaEngine, state: InodeAllocatorState) {
    local
        .write_counters(|counters| {
            PgMetaEngine::persist_inode_allocator_state_locked(counters, state)
        })
        .expect("persist inode allocator state");
}

fn persist_legacy_inode_range_state(local: &RedbMetaEngine, state: LocalIdRangeState) {
    local
        .write_counters(|counters| {
            PgMetaEngine::persist_local_id_range_state_locked(
                counters,
                LEGACY_INODE_RANGE_SPEC,
                state,
            )
        })
        .expect("persist legacy inode range state");
}

fn load_inode_state(local: &RedbMetaEngine) -> InodeAllocatorState {
    PgMetaEngine::load_persisted_inode_allocator_state(local)
        .expect("load persisted inode allocator state")
}

#[test]
fn prime_local_id_ranges_persists_initial_inode_window_when_missing() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _inode_window_override = set_inode_window_reservation_override([Ok(InodeWindow {
        start: 8_000,
        next: 8_000,
        end: 258_000,
    })]);

    PgMetaEngine::persist_client_slot(&pg.local, 3).expect("persist client slot");
    persist_inode_state(&pg.local, InodeAllocatorState::default());
    pg.prime_local_id_ranges(3)
        .expect("prime local id allocator state");

    let inode_state = load_inode_state(&pg.local);

    assert!(
        inode_state.current
            == InodeWindow {
                start: 8_000,
                next: 8_000,
                end: 258_000,
            },
        "prime must persist the first inode window locally when none exists"
    );
    assert!(inode_state.prefetched.is_empty());
}

#[test]
fn prime_local_id_ranges_migrates_legacy_inode_ranges_to_durable_windows() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    persist_inode_state(&pg.local, InodeAllocatorState::default());
    persist_legacy_inode_range_state(
        &pg.local,
        LocalIdRangeState {
            next: 1_203,
            active_start: 1_000,
            active_end: 2_000,
            reserved_end: 3_000,
        },
    );

    pg.prime_local_id_ranges(0)
        .expect("prime must migrate legacy inode state");

    assert_eq!(
        load_inode_state(&pg.local),
        InodeAllocatorState {
            current: InodeWindow {
                start: 1_000,
                next: 1_203,
                end: 2_000,
            },
            prefetched: InodeWindow {
                start: 2_000,
                next: 2_000,
                end: 3_000,
            },
        },
        "legacy inode ranges should be migrated into durable current/prefetched windows"
    );
}

#[test]
fn open_with_persisted_slot_and_ranges_survives_pg_unavailable() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "offline-reopen",
            "offline-reopen-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set format info");

    PgMetaEngine::persist_client_slot(&local, 5).expect("persist client slot");
    persist_inode_state(
        &local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 4_000,
                next: 4_002,
                end: 4_008,
            },
            prefetched: InodeWindow::default(),
        },
    );
    drop(local);

    let reopened = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
        },
    );
    let reopened = reopened.expect("reopen from persisted allocator state");
    assert_eq!(
        load_inode_state(&reopened.local).current,
        InodeWindow {
            start: 4_000,
            next: 4_002,
            end: 4_008,
        },
        "reopen must reuse the persisted current inode window without reserving a fresh one"
    );
}

#[test]
fn sfs315_reopen_consumes_persisted_inode_and_object_headroom_without_pg() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "sfs315-offline-reopen-headroom",
            "sfs315-offline-reopen-headroom-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set format info");

    PgMetaEngine::persist_client_slot(&local, 5).expect("persist client slot");
    persist_inode_state(
        &local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 8_000,
                next: 8_001,
                end: 8_005,
            },
            prefetched: InodeWindow::default(),
        },
    );
    drop(local);

    let reopened = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("reopen from persisted local allocation state");

    let status = MetaReplication::metadata_authority_status(&*reopened);
    assert!(
        status.inode_id_headroom,
        "reopened allocator should report remaining inode headroom"
    );
    assert!(
        status.object_id_headroom,
        "reopened allocator should report remaining object ID headroom separately"
    );

    let ino = reopened
        .next_inode()
        .expect("allocate inode from persisted headroom");
    assert_eq!(ino, 8_001);
    let object_ids = reopened
        .next_object_ids(2)
        .expect("allocate object IDs from persisted headroom");
    assert_eq!(object_ids, vec![8_002, 8_003]);

    assert_eq!(
        load_inode_state(&reopened.local).current.next,
        8_004,
        "inode/object allocation progress must persist after offline reopen"
    );
}

#[test]
fn sfs315_failed_object_batch_preserves_smaller_inode_headroom() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _inode_window_override =
        set_inode_window_reservation_override([Err("pg local inode window exhausted".into())]);

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 9_000,
                next: 9_000,
                end: 9_002,
            },
            prefetched: InodeWindow::default(),
        },
    );
    pg.update_inode_headroom_flag(load_inode_state(&pg.local));

    let object_error = pg
        .next_object_ids(3)
        .expect_err("object batch should fail when it cannot fit in local headroom");
    assert!(
        object_error
            .to_string()
            .contains("failed to ensure current local inode window"),
        "object batch failure should report refill failure without declaring smaller local headroom exhausted: {object_error}"
    );

    let status = MetaReplication::metadata_authority_status(&pg);
    assert!(
        status.inode_id_headroom,
        "failed larger object batch must not consume the smaller inode headroom that remains"
    );
    assert!(
        status.object_id_headroom,
        "object ID status should still expose the remaining smaller local ID headroom"
    );

    let ino = pg
        .next_inode()
        .expect("smaller inode allocation should still use remaining local headroom");
    assert_eq!(ino, 9_000);
    assert_eq!(
        load_inode_state(&pg.local).current.next,
        9_001,
        "failed object batch must not burn the tail of the persisted window"
    );
}

#[test]
fn open_with_persisted_slot_and_legacy_inode_ranges_survives_pg_unavailable() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "offline-reopen-legacy-inodes",
            "offline-reopen-legacy-inodes-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set format info");

    PgMetaEngine::persist_client_slot(&local, 5).expect("persist client slot");
    let legacy_inode_base =
        PgMetaEngine::reserve_counter_range_base(5, LEGACY_PG_INODE_ID_STRIDE, ROOT_INO + 1);
    persist_legacy_inode_range_state(
        &local,
        LocalIdRangeState {
            next: legacy_inode_base + 2,
            active_start: legacy_inode_base,
            active_end: legacy_inode_base + LEGACY_PG_INODE_RANGE_WINDOW,
            reserved_end: legacy_inode_base + LEGACY_PG_INODE_RANGE_WINDOW * 2,
        },
    );
    drop(local);

    let reopened = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
        },
    );
    let reopened = reopened.expect("reopen from persisted legacy inode state");
    assert_eq!(
        load_inode_state(&reopened.local),
        InodeAllocatorState {
            current: InodeWindow {
                start: legacy_inode_base,
                next: legacy_inode_base + 2,
                end: legacy_inode_base + LEGACY_PG_INODE_RANGE_WINDOW,
            },
            prefetched: InodeWindow {
                start: legacy_inode_base + LEGACY_PG_INODE_RANGE_WINDOW,
                next: legacy_inode_base + LEGACY_PG_INODE_RANGE_WINDOW,
                end: legacy_inode_base + LEGACY_PG_INODE_RANGE_WINDOW * 2,
            },
        },
        "offline reopen must accept legacy inode ranges and migrate them into durable windows"
    );
}

#[test]
fn open_with_persisted_slot_without_ranges_still_requires_pg() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "offline-reopen-no-ranges",
            "offline-reopen-no-ranges-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set format info");
    PgMetaEngine::persist_client_slot(&local, 5).expect("persist client slot");
    drop(local);

    let reopened = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
        },
    );
    assert!(
        reopened.is_err(),
        "offline reopen must not mint fresh headroom from a legacy client slot without persisted ranges"
    );
}

#[test]
fn open_with_persisted_ranges_does_not_mask_non_transient_schema_errors() {
    let _schema_error = set_schema_ensure_test_override(
        "postgres file lock lease schema ensure failed: permission denied (sqlstate=42501)",
    );
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "offline-reopen-sqlstate",
            "offline-reopen-sqlstate-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set format info");

    PgMetaEngine::persist_client_slot(&local, 5).expect("persist client slot");
    persist_inode_state(
        &local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 7_000,
                next: 7_002,
                end: 7_008,
            },
            prefetched: InodeWindow::default(),
        },
    );
    drop(local);

    let reopened = PgMetaEngine::open_with_options(
        &path,
        PgOpenOptions {
            background_poll: false,
        },
    );
    assert!(
        reopened.is_err(),
        "non-transient schema/bootstrap failures must not be silently treated as offline-mode reopens"
    );
}

#[test]
fn next_inode_switches_to_prefetched_window_without_pg_roundtrip() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 100,
                next: 101,
                end: 102,
            },
            prefetched: InodeWindow {
                start: 500,
                next: 500,
                end: 503,
            },
        },
    );

    let allocated = pg.next_inode().expect("allocate boundary inode");
    assert_eq!(allocated, 101);
    assert_eq!(
        load_inode_state(&pg.local),
        InodeAllocatorState {
            current: InodeWindow {
                start: 500,
                next: 500,
                end: 503,
            },
            prefetched: InodeWindow::default(),
        },
        "exhausting the current window must switch to the prefetched window locally"
    );
}

#[test]
fn next_object_ids_share_inode_allocation_stream() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 100,
                next: 100,
                end: 110,
            },
            prefetched: InodeWindow::default(),
        },
    );

    let file_ino = pg.next_inode().expect("allocate inode");
    let object_ids = pg.next_object_ids(3).expect("allocate object IDs");
    let next_ino = pg.next_inode().expect("allocate next inode");

    assert_eq!(file_ino, 100);
    assert_eq!(object_ids, vec![101, 102, 103]);
    assert_eq!(next_ino, 104);
    assert_eq!(load_inode_state(&pg.local).current.next, 105);
}

#[test]
fn next_object_ids_preserves_contiguous_batch_at_window_boundary() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 100,
                next: 101,
                end: 102,
            },
            prefetched: InodeWindow {
                start: 500,
                next: 500,
                end: 503,
            },
        },
    );

    let object_ids = pg.next_object_ids(2).expect("allocate object IDs");

    assert_eq!(object_ids, vec![500, 501]);
    assert_eq!(load_inode_state(&pg.local).current.next, 502);
}

#[test]
fn next_object_ids_reserves_new_window_when_tail_is_too_small() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _inode_window_override = set_inode_window_reservation_override([Ok(InodeWindow {
        start: 500,
        next: 500,
        end: 510,
    })]);

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 100,
                next: 100,
                end: 102,
            },
            prefetched: InodeWindow::default(),
        },
    );

    let object_ids = pg.next_object_ids(3).expect("allocate object IDs");

    assert_eq!(object_ids, vec![500, 501, 502]);
    assert_eq!(load_inode_state(&pg.local).current.next, 503);
}

#[test]
fn sfs315_large_object_batch_replaces_undersized_prefetched_window() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _inode_window_override = set_inode_window_reservation_override([
        Ok(InodeWindow {
            start: 500,
            next: 500,
            end: 510,
        }),
        Err("second reservation should not be needed".into()),
    ]);

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 100,
                next: 101,
                end: 102,
            },
            prefetched: InodeWindow {
                start: 200,
                next: 200,
                end: 202,
            },
        },
    );

    let object_ids = pg
        .next_object_ids(3)
        .expect("allocate object IDs from a replacement window");

    assert_eq!(object_ids, vec![500, 501, 502]);
    assert_eq!(
        load_inode_state(&pg.local).current.next,
        503,
        "a too-small prefetched window must not block applying a larger replacement"
    );
}

#[test]
fn next_object_ids_allows_batch_larger_than_default_window() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let count = PG_INODE_WINDOW_SIZE + 1;
    let _inode_window_override = set_inode_window_reservation_override([Ok(InodeWindow {
        start: 500,
        next: 500,
        end: 500 + count,
    })]);

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 100,
                next: 100,
                end: 102,
            },
            prefetched: InodeWindow::default(),
        },
    );

    let object_ids = pg
        .next_object_ids(u32::try_from(count).expect("test count fits u32"))
        .expect("allocate object IDs");

    assert_eq!(object_ids.len(), count as usize);
    assert_eq!(object_ids.first().copied(), Some(500));
    assert_eq!(object_ids.last().copied(), Some(500 + count - 1));
    assert_eq!(load_inode_state(&pg.local).current.next, 500 + count);
}

#[test]
fn next_object_ids_reports_inode_window_exhaustion() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _inode_window_override =
        set_inode_window_reservation_override([Err("pg local inode window exhausted".into())]);

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 3_000,
                next: 3_001,
                end: 3_001,
            },
            prefetched: InodeWindow::default(),
        },
    );

    let error = pg
        .next_object_ids(1)
        .expect_err("object allocation must fail when the shared inode window is exhausted");

    assert!(
        error
            .to_string()
            .contains("pg local inode window exhausted"),
        "unexpected error: {error}"
    );
}

#[test]
fn next_inode_prefetches_single_future_window_at_low_watermark() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _inode_window_override = set_inode_window_reservation_override([Ok(InodeWindow {
        start: 9_000,
        next: 9_000,
        end: 59_500,
    })]);

    let start = 1_000;
    let end = start + PG_INODE_PREFETCH_LOW_WATERMARK + 1;
    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start,
                next: start,
                end,
            },
            prefetched: InodeWindow::default(),
        },
    );

    let allocated = pg
        .next_inode()
        .expect("allocate and trigger low-water prefetch");
    assert_eq!(allocated, start);

    let state = load_inode_state(&pg.local);
    assert_eq!(state.current.next, start + 1);
    assert_eq!(
        state.prefetched,
        InodeWindow {
            start: 9_000,
            next: 9_000,
            end: 59_500,
        },
        "reaching the low-watermark should reserve exactly one prefetched inode window"
    );
}

#[test]
fn next_inode_reserves_replacement_current_window_when_pg_is_available() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _inode_window_override = set_inode_window_reservation_override([Ok(InodeWindow {
        start: 9_000,
        next: 9_000,
        end: 70_000,
    })]);

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 4_000,
                next: 4_000,
                end: 4_001,
            },
            prefetched: InodeWindow::default(),
        },
    );

    let first = pg
        .next_inode()
        .expect("allocate final inode before current-window refill");
    assert_eq!(first, 4_000);
    assert!(
        load_inode_state(&pg.local).current
            == InodeWindow {
                start: 9_000,
                next: 9_000,
                end: 70_000,
            },
        "when PostgreSQL is available, exhausting the current window should refill it immediately"
    );
}

#[test]
fn create_file_respects_inode_headroom_exhaustion() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _inode_window_override =
        set_inode_window_reservation_override([Err("pg local inode window exhausted".into())]);

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 3_000,
                next: 3_001,
                end: 3_001,
            },
            prefetched: InodeWindow::default(),
        },
    );
    pg.update_inode_headroom_flag(load_inode_state(&pg.local));

    let result = pg.create_file(ROOT_INO, b"exhausted.txt", 0, 0, 0o644);
    assert!(
        matches!(result, Err(MetaError::Storage(ref message)) if message.contains("inode window exhausted")),
        "create_file must honor inode headroom exhaustion instead of bypassing the PG allocator: {result:?}"
    );
}

#[test]
fn next_inode_reports_read_only_headroom_once_windows_are_exhausted() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _inode_window_override = set_inode_window_reservation_override([
        Err("pg local inode window exhausted".into()),
        Err("pg local inode window exhausted".into()),
    ]);

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 5_000,
                next: 5_000,
                end: 5_001,
            },
            prefetched: InodeWindow::default(),
        },
    );
    let first = pg.next_inode().expect("final inode within local headroom");
    assert_eq!(first, 5_000);

    let error = pg
        .next_inode()
        .expect_err("headroom should hard-stop new inode allocation once no local window remains");
    assert!(
        matches!(error, MetaError::Storage(ref message) if message.contains("inode window exhausted")),
        "unexpected exhaustion error: {error}"
    );

    let authority = MetaReplication::metadata_authority_status(&pg);
    assert!(
        !authority.allows_new_mutations(),
        "inode window exhaustion should surface a read-only metadata authority status"
    );
}

#[test]
fn next_inode_distinguishes_pg_reservation_failures_from_true_exhaustion() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let _inode_window_override =
        set_inode_window_reservation_override([Err("connect timeout".into())]);

    persist_inode_state(
        &pg.local,
        InodeAllocatorState {
            current: InodeWindow {
                start: 6_000,
                next: 6_001,
                end: 6_001,
            },
            prefetched: InodeWindow::default(),
        },
    );
    pg.update_inode_headroom_flag(load_inode_state(&pg.local));

    let error = pg
        .next_inode()
        .expect_err("allocation must report the PostgreSQL reservation failure");
    assert!(
        matches!(error, MetaError::Storage(ref message) if message.contains("failed to reserve a new inode window from PostgreSQL after exhausting local inode windows")),
        "unexpected PostgreSQL reservation error: {error}"
    );
}

#[test]
fn persisted_pg_flushed_event_seq_roundtrips_in_local_counter() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local)
            .expect("initial flushed event seq"),
        0
    );
    pg.persist_flushed_event_seq(19)
        .expect("persist flushed event seq");
    assert_eq!(pg.flushed_event_seq.load(Ordering::SeqCst), 19);
    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local)
            .expect("reload flushed event seq"),
        19
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_file_lock_lease_blocks_peer_until_release() {
    let _guard = file_lock_test_guard();
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-file-lock-blocks-peer");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open_with_options(
        &db_a,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open a");
    let b = PgMetaEngine::open_with_options(
        &db_b,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open b");
    let file = create_published_small_file(&a, ROOT_INO, b"locked.txt", 50_201);
    a.flush_metadata().expect("flush file create");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"locked.txt",
        Duration::from_secs(10)
    ));

    let ttl = Duration::from_secs(2);
    let acquired = a
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        .expect("a acquire exclusive lease");
    assert!(matches!(acquired, FileLockLeaseAcquireOutcome::Acquired(_)));

    let blocked = b
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        .expect("b conflicting acquire result");
    assert!(matches!(blocked, FileLockLeaseAcquireOutcome::Blocked(_)));

    let released = a
        .release_file_lock_lease(file.ino)
        .expect("a release lease");
    assert!(matches!(
        released,
        FileLockLeaseReleaseOutcome::Released(None)
    ));

    let acquired_after_release = b
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        .expect("b acquire after release");
    assert!(matches!(
        acquired_after_release,
        FileLockLeaseAcquireOutcome::Acquired(_)
    ));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_file_lock_first_acquire_serializes_concurrent_racers() {
    let _guard = file_lock_test_guard();
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-file-lock-first-acquire-race");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open_with_options(
        &db_a,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open a");
    let b = PgMetaEngine::open_with_options(
        &db_b,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open b");
    let file = create_published_small_file(&a, ROOT_INO, b"first-acquire-race.txt", 50_202);
    a.flush_metadata().expect("flush file create");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"first-acquire-race.txt",
        Duration::from_secs(10)
    ));

    let ttl = Duration::from_secs(2);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let barrier_a = Arc::clone(&barrier);
    let barrier_b = Arc::clone(&barrier);
    let a2 = Arc::clone(&a);
    let b2 = Arc::clone(&b);

    let acquire_a = std::thread::spawn(move || {
        barrier_a.wait();
        a2.acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
    });
    let acquire_b = std::thread::spawn(move || {
        barrier_b.wait();
        b2.acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
    });

    let outcome_a = acquire_a
        .join()
        .expect("join acquire a")
        .expect("acquire a");
    let outcome_b = acquire_b
        .join()
        .expect("join acquire b")
        .expect("acquire b");
    let acquired_count = [outcome_a, outcome_b]
        .into_iter()
        .filter(|outcome| matches!(outcome, FileLockLeaseAcquireOutcome::Acquired(_)))
        .count();
    assert_eq!(
        acquired_count, 1,
        "exactly one concurrent first-acquire attempt should win the lease row"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_open_recreates_missing_file_lock_lease_table() {
    let _guard = file_lock_test_guard();
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = tempdir().expect("tempdir");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-open-recreates-file-lock-lease-table");
    let info = volume.format_info();
    seed_local(&db, &info);
    drop_file_lock_lease_table(&dsn);

    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open pg engine after dropping file_lock_leases");
    let file = engine
        .create_file(ROOT_INO, b"recreated-lock-table.txt", 0, 0, 0o644)
        .expect("create file");

    let acquired = engine
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, Duration::from_secs(2))
        .expect("acquire lease after open-time schema ensure");
    assert!(matches!(acquired, FileLockLeaseAcquireOutcome::Acquired(_)));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_file_lock_lease_expires_for_peer() {
    let _guard = file_lock_test_guard();
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-file-lock-expiry");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open_with_options(
        &db_a,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open a");
    let b = PgMetaEngine::open_with_options(
        &db_b,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open b");
    let file = create_published_small_file(&a, ROOT_INO, b"expiring.txt", 50_203);
    a.flush_metadata().expect("flush file create");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"expiring.txt",
        Duration::from_secs(10)
    ));

    let ttl = Duration::from_millis(150);
    let acquired = a
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        .expect("a acquire lease");
    assert!(matches!(acquired, FileLockLeaseAcquireOutcome::Acquired(_)));

    std::thread::sleep(Duration::from_millis(350));

    let peer_acquired = b
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        .expect("b acquire after expiry");
    assert!(matches!(
        peer_acquired,
        FileLockLeaseAcquireOutcome::Acquired(_)
    ));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_file_lock_lease_renew_keeps_peer_blocked() {
    let _guard = file_lock_test_guard();
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-file-lock-renew");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open_with_options(
        &db_a,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open a");
    let b = PgMetaEngine::open_with_options(
        &db_b,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open b");
    let file = create_published_small_file(&a, ROOT_INO, b"renew.txt", 50_204);
    a.flush_metadata().expect("flush file create");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"renew.txt",
        Duration::from_secs(10)
    ));

    let ttl = Duration::from_secs(3);
    let acquired = a
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        .expect("a acquire lease");
    assert!(matches!(acquired, FileLockLeaseAcquireOutcome::Acquired(_)));

    std::thread::sleep(Duration::from_millis(500));

    let renewed = a
        .renew_file_lock_lease(file.ino, ttl)
        .expect("a renew lease");
    assert!(
        matches!(renewed, FileLockLeaseRenewOutcome::Renewed(_)),
        "expected renewed lease, got {renewed:?}"
    );

    std::thread::sleep(Duration::from_secs(1));

    let blocked = b
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        .expect("b conflicting acquire after renew");
    assert!(matches!(blocked, FileLockLeaseAcquireOutcome::Blocked(_)));

    std::thread::sleep(Duration::from_millis(3500));

    let acquired_after_renewed_expiry = b
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        .expect("b acquire after renewed expiry");
    assert!(matches!(
        acquired_after_renewed_expiry,
        FileLockLeaseAcquireOutcome::Acquired(_)
    ));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_file_lock_known_state_refreshes_from_notify() {
    let _guard = file_lock_test_guard();
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let _poll_guard = set_test_env("STUDIOFS_PG_SYNC_POLL_SECS", "3600");

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-file-lock-notify-refresh");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");
    let file = create_published_small_file(&a, ROOT_INO, b"notify.txt", 50_205);
    a.flush_metadata().expect("flush file create");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"notify.txt",
        Duration::from_secs(10)
    ));

    let ttl = Duration::from_secs(2);
    a.acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        .expect("a acquire lease");

    let blocked = b
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        .expect("b conflicting acquire result");
    assert!(matches!(blocked, FileLockLeaseAcquireOutcome::Blocked(_)));
    assert!(
        b.known_file_lock_lease(file.ino).is_some(),
        "blocked peer should cache the known conflicting lease"
    );

    a.release_file_lock_lease(file.ino)
        .expect("a release lease");

    assert!(
        wait_until(Duration::from_secs(10), || b
            .known_file_lock_lease(file.ino)
            .is_none()),
        "peer known lock cache should clear after NOTIFY-driven refresh"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_passive_lock_monitoring_starts_notify_listener_but_defers_renew_worker() {
    let _guard = file_lock_test_guard();
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = tempdir().expect("tempdir");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-lock-workers-lazy-start");
    let info = volume.format_info();
    seed_local(&db, &info);

    let engine = PgMetaEngine::open(&db).expect("open engine");
    assert!(
        engine
            .lock_notify_handle
            .lock()
            .expect("lock notify handle")
            .is_none(),
        "open alone should not eagerly start the pg lock notify listener"
    );
    assert!(
        engine
            .lock_renew_handle
            .lock()
            .expect("lock renew handle")
            .is_none(),
        "open should still defer the pg lock renew worker until a lock is held"
    );

    engine
        .ensure_known_file_lock_monitoring()
        .expect("start passive lock monitoring");
    assert!(
        engine
            .lock_notify_handle
            .lock()
            .expect("lock notify handle after passive monitoring")
            .is_some(),
        "passive lock monitoring should start the pg lock notify listener"
    );
    assert!(
        engine
            .lock_renew_handle
            .lock()
            .expect("lock renew handle after passive monitoring")
            .is_none(),
        "passive lock monitoring should not eagerly start the pg lock renew worker"
    );

    let file = engine
        .create_file(ROOT_INO, b"lazy-lock-workers.txt", 0, 0, 0o644)
        .expect("create file");
    let acquired = engine
        .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, Duration::from_secs(2))
        .expect("acquire lease");
    assert!(matches!(acquired, FileLockLeaseAcquireOutcome::Acquired(_)));
    assert!(
        engine
            .lock_notify_handle
            .lock()
            .expect("lock notify handle after acquire")
            .is_some(),
        "first lock usage should start the pg lock notify listener"
    );
    assert!(
        engine
            .lock_renew_handle
            .lock()
            .expect("lock renew handle after acquire")
            .is_some(),
        "first lock usage should start the pg lock renew worker"
    );
}

#[test]
fn replication_backlog_reports_pending_and_inflight_ops() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        state.ops = 7;
    }
    pg.inflight_replication_ops.store(3, Ordering::SeqCst);

    let backlog = pg.replication_backlog();
    assert_eq!(backlog.pending_ops, 7);
    assert_eq!(backlog.inflight_ops, 3);
    assert_eq!(backlog.total_ops(), 10);
}

#[test]
fn upload_progress_only_mutation_does_not_create_pg_replication_backlog() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"upload-progress-only.bin", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 4096, 1);
    let payload = PayloadRef::new_partial(77_001, 4096);
    pg.stage_payload_slice(file.ino, 0, &payload)
        .expect("stage local overlay");

    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack setup events");
    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        PgMetaEngine::clear_pending_state(&mut state);
    }
    let (wakeup_lock, _) = &*pg.poll_wakeup;
    {
        let mut wakeup = wakeup_lock.lock().expect("poll wakeup lock");
        *wakeup = false;
    }

    assert!(
        pg.mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 1, payload.object_id)
            .expect("mark uploaded"),
        "upload marker must update the local overlay"
    );
    let after_upload_progress = pg
        .local
        .get_counter("nextEventSeq")
        .expect("after upload progress");

    let backlog = pg.replication_backlog();
    assert_eq!(
        backlog.total_ops(),
        0,
        "local-only upload progress must not appear as remote metadata backlog"
    );
    assert_eq!(
        pg.flushed_event_seq.load(Ordering::SeqCst),
        after_upload_progress,
        "the local-only event cursor should be acknowledged so reopen does not rebuild backlog"
    );
    assert!(
        !*wakeup_lock.lock().expect("poll wakeup lock"),
        "local-only upload progress must not wake the metadata poll worker"
    );
}

#[test]
fn published_unlink_creates_remote_visible_pg_replication_backlog() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    create_published_small_file(&pg, ROOT_INO, b"published-delete.bin", 77_002);
    let published_end = pg.local.get_counter("nextEventSeq").expect("published end");
    pg.persist_flushed_event_seq(published_end)
        .expect("ack published setup");
    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        PgMetaEngine::clear_pending_state(&mut state);
    }

    pg.unlink(ROOT_INO, b"published-delete.bin")
        .expect("unlink published file");
    pg.request_eager_flush();

    let backlog = pg.replication_backlog();
    assert!(
        backlog.total_ops() > 0,
        "published unlink must create remote-visible metadata backlog"
    );
    let batch = pg
        .claim_pending_batch(false)
        .expect("published unlink should be claimable after eager flush request");
    let events = pg
        .local
        .read_events_range(batch.event_start, batch.event_end)
        .expect("read unlink events");
    let projected = RemoteApplyCoordinator::project_remote_events(&events);
    assert!(
        projected
            .iter()
            .any(|event| matches!(event, MetaEvent::RemoveEntry { .. })),
        "published unlink must project a remote RemoveEntry"
    );
    assert!(
        projected
            .iter()
            .any(|event| matches!(event, MetaEvent::RemoveAttr { .. })),
        "published unlink must project a remote RemoveAttr"
    );
}

#[test]
fn request_eager_flush_wakes_pg_poll_worker() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.request_pending_flush();
    let (wakeup_lock, _) = &*pg.poll_wakeup;
    assert!(
        !*wakeup_lock.lock().expect("poll wakeup lock"),
        "regular upload-pipeline flush requests should not wake the metadata poll worker"
    );

    pg.request_eager_flush();

    assert!(
        *wakeup_lock.lock().expect("poll wakeup lock"),
        "namespace eager flush requests should wake the metadata poll worker"
    );
}

#[test]
fn published_base_graph_wakes_pg_poll_worker() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"publish-wakeup.bin", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 4096, 1);
    let payload = PayloadRef::new_partial(77_003, 4096);
    pg.stage_payload_slice(file.ino, 0, &payload)
        .expect("stage local overlay");
    assert!(
        pg.mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 1, payload.object_id)
            .expect("mark uploaded"),
        "upload marker must update the local overlay"
    );

    let (wakeup_lock, _) = &*pg.poll_wakeup;
    {
        let mut wakeup = wakeup_lock.lock().expect("poll wakeup lock");
        *wakeup = false;
    }

    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish base graph"),
        "base graph publish must succeed"
    );

    assert!(
        *wakeup_lock.lock().expect("poll wakeup lock"),
        "publishing a remote-visible base graph should wake the metadata poll worker"
    );
}

#[test]
fn ready_file_attr_change_wakes_pg_poll_worker() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"ready-attr-wakeup.bin", 0, 0, 0o644)
        .expect("create file");
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack setup events");
    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        PgMetaEngine::clear_pending_state(&mut state);
    }

    let (wakeup_lock, _) = &*pg.poll_wakeup;
    {
        let mut wakeup = wakeup_lock.lock().expect("poll wakeup lock");
        *wakeup = false;
    }

    let mut attr = pg.get_attr(file.ino).expect("get attr");
    attr.mtime = attr.mtime.saturating_add(1);
    attr.touch_ctime();
    pg.set_attr(&attr).expect("set ready attr");

    assert!(
        *wakeup_lock.lock().expect("poll wakeup lock"),
        "settled file metadata changes should wake the metadata poll worker"
    );
}

#[test]
fn xattr_change_batches_without_pg_poll_wakeup() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"xattr-batch-wakeup.bin", 0, 0, 0o644)
        .expect("create file");
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack setup events");
    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        PgMetaEngine::clear_pending_state(&mut state);
    }

    let (wakeup_lock, _) = &*pg.poll_wakeup;
    {
        let mut wakeup = wakeup_lock.lock().expect("poll wakeup lock");
        *wakeup = false;
    }

    pg.set_xattr(file.ino, b"user.color", b"blue", 0)
        .expect("set xattr");

    assert_eq!(
        pg.replication_backlog().pending_ops,
        1,
        "xattr changes remain remote-visible metadata"
    );
    assert!(
        !*wakeup_lock.lock().expect("poll wakeup lock"),
        "xattr overlay changes should batch instead of waking PG before an explicit flush"
    );
}

#[test]
fn uploading_file_attr_change_batches_without_pg_poll_wakeup() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    let file = pg
        .create_file(ROOT_INO, b"uploading-attr-batch.bin", 0, 0, 0o644)
        .expect("create file");
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    pg.persist_flushed_event_seq(setup_end)
        .expect("ack setup events");
    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        PgMetaEngine::clear_pending_state(&mut state);
    }

    let (wakeup_lock, _) = &*pg.poll_wakeup;
    {
        let mut wakeup = wakeup_lock.lock().expect("poll wakeup lock");
        *wakeup = false;
    }

    let mut attr = pg.get_attr(file.ino).expect("get attr");
    attr.size = 4096;
    attr.committed_size = 0;
    attr.upload_state = UploadState::Uploading;
    attr.touch_ctime();
    pg.set_attr(&attr).expect("set uploading attr");

    assert_eq!(
        pg.replication_backlog().pending_ops,
        1,
        "uploading attrs remain remote-visible metadata"
    );
    assert!(
        !*wakeup_lock.lock().expect("poll wakeup lock"),
        "upload-progress attrs should batch instead of waking PG on the hot write path"
    );
}

#[test]
fn unit_remote_projection_preserves_nested_base_graph_publish() {
    let (_writer_dir, writer) = temp_pg_engine_for_unit();
    let (_remote_dir, remote) = temp_pg_engine_for_unit();

    let start = writer.local.get_counter("nextEventSeq").expect("start seq");
    let dir = writer
        .create_dir(ROOT_INO, b"nested", 0, 0, 0o755)
        .expect("create dir");
    let file = writer
        .create_file(dir.ino, b"leaf.bin", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&writer, file.ino, 4096, 1);
    let block = PayloadRef::new_partial(701, 4096);
    writer
        .stage_payload_slice(file.ino, 0, &block)
        .expect("set payload slice");
    mark_overlay_objects_uploaded(&writer, file.ino, 1, &[block.object_id]);
    assert!(
        writer
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish base graph"),
        "publish must succeed"
    );
    let end = writer.local.get_counter("nextEventSeq").expect("end seq");

    apply_projected_remote_range(&writer, &remote, start, end);

    let remote_dir = remote.lookup(ROOT_INO, b"nested").expect("lookup dir");
    let remote_file = remote
        .lookup(remote_dir.ino, b"leaf.bin")
        .expect("lookup file");
    assert_eq!(remote_file.ino, file.ino);
    let remote_attr = remote.get_attr(remote_file.ino).expect("get remote attr");
    assert_eq!(remote_attr.size, 4096);
    assert_eq!(remote_attr.committed_size, 4096);
    assert_eq!(remote_attr.committed_content_epoch, 1);
    assert_eq!(remote_attr.upload_state, UploadState::Ready);
    assert_eq!(
        lookup_object_ids(&remote, remote_file.ino, remote_attr.size).expect("remote object ids"),
        vec![block.object_id]
    );
}

#[test]
fn unit_remote_projection_base_graph_publish_replaces_base_graph_on_shrink() {
    let (_writer_dir, writer) = temp_pg_engine_for_unit();
    let (_remote_dir, remote) = temp_pg_engine_for_unit();
    let block_size = DEFAULT_BLOCK_SIZE as u64;

    let batch1_start = writer
        .local
        .get_counter("nextEventSeq")
        .expect("batch1 start");
    let file = writer
        .create_file(ROOT_INO, b"shrink.bin", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&writer, file.ino, block_size + 4096, 1);
    let first = PayloadRef::new_full(1001, DEFAULT_BLOCK_SIZE);
    let second = PayloadRef::new_partial(block_size + 2, 4096);
    writer
        .stage_payload_slice(file.ino, 0, &first)
        .expect("set first block");
    writer
        .stage_payload_slice(file.ino, 1, &second)
        .expect("set second block");
    mark_overlay_objects_uploaded(&writer, file.ino, 1, &[first.object_id, second.object_id]);
    assert!(
        writer
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish initial base graph"),
        "initial base graph publish must succeed"
    );
    let batch1_end = writer
        .local
        .get_counter("nextEventSeq")
        .expect("batch1 end");
    apply_projected_remote_range(&writer, &remote, batch1_start, batch1_end);

    assert_eq!(
        lookup_object_ids(&remote, file.ino, block_size + 4096).expect("remote initial object ids"),
        vec![first.object_id, second.object_id]
    );

    let batch2_start = batch1_end;
    set_file_state(&writer, file.ino, 4096, 2);
    writer
        .stage_payload_tombstone(file.ino, 1)
        .expect("remove second block for shrink");
    let replacement = PayloadRef::new_partial(block_size + 3, 4096);
    writer
        .stage_payload_slice(file.ino, 0, &replacement)
        .expect("set replacement payload slice");
    mark_overlay_objects_uploaded(&writer, file.ino, 2, &[replacement.object_id]);
    assert!(
        writer
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 2)
            .expect("publish shrink base graph"),
        "shrink base graph publish must succeed"
    );
    let batch2_end = writer
        .local
        .get_counter("nextEventSeq")
        .expect("batch2 end");
    apply_projected_remote_range(&writer, &remote, batch2_start, batch2_end);

    let remote_attr = remote.get_attr(file.ino).expect("remote attr after shrink");
    assert_eq!(remote_attr.size, 4096);
    assert_eq!(remote_attr.committed_size, 4096);
    assert_eq!(remote_attr.committed_content_epoch, 2);
    assert_eq!(remote_attr.upload_state, UploadState::Ready);
    assert_eq!(
        lookup_object_ids(&remote, file.ino, remote_attr.size)
            .expect("remote object ids after shrink"),
        vec![replacement.object_id],
        "base graph publish must replace the full remote base graph and drop stale trailing blocks"
    );
}

#[test]
fn unit_remote_projection_updates_nested_file_after_temp_rename_then_publish() {
    let (_writer_dir, writer) = temp_pg_engine_for_unit();
    let (_remote_dir, remote) = temp_pg_engine_for_unit();
    let block_size = DEFAULT_BLOCK_SIZE as u64;

    let batch1_start = writer
        .local
        .get_counter("nextEventSeq")
        .expect("batch1 start");
    let dir = writer
        .create_dir(ROOT_INO, b"drafts", 0, 0, 0o755)
        .expect("create dir");
    let file = writer
        .create_file(dir.ino, b"temp.bin", 0, 0, 0o644)
        .expect("create temp file");
    writer
        .rename(dir.ino, b"temp.bin", dir.ino, b"final.bin")
        .expect("rename temp to final");
    let batch1_end = writer
        .local
        .get_counter("nextEventSeq")
        .expect("batch1 end");

    apply_projected_remote_range(&writer, &remote, batch1_start, batch1_end);

    let remote_dir = remote
        .lookup(ROOT_INO, b"drafts")
        .expect("lookup dir after rename");
    assert!(matches!(
        remote.lookup(remote_dir.ino, b"final.bin"),
        Err(MetaError::EntryNotFound { .. })
    ));

    let batch2_start = batch1_end;
    set_file_state(&writer, file.ino, block_size + 4096, 1);
    let block0 = PayloadRef::new_full(801, DEFAULT_BLOCK_SIZE);
    let block1 = PayloadRef::new_partial(802, 4096);
    writer
        .stage_payload_slice(file.ino, 0, &block0)
        .expect("set payload slice 0");
    writer
        .stage_payload_slice(file.ino, 1, &block1)
        .expect("set payload slice 1");
    mark_overlay_objects_uploaded(&writer, file.ino, 1, &[block0.object_id, block1.object_id]);
    assert!(
        writer
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish renamed base graph"),
        "publish after rename must succeed"
    );
    let batch2_end = writer
        .local
        .get_counter("nextEventSeq")
        .expect("batch2 end");

    apply_projected_remote_range(&writer, &remote, batch2_start, batch2_end);

    let remote_file_after = remote
        .lookup(remote_dir.ino, b"final.bin")
        .expect("lookup final after publish");
    assert_eq!(remote_file_after.ino, file.ino);
    let remote_attr_after = remote
        .get_attr(remote_file_after.ino)
        .expect("attr after publish");
    assert_eq!(remote_attr_after.size, block_size + 4096);
    assert_eq!(remote_attr_after.committed_size, block_size + 4096);
    assert_eq!(remote_attr_after.upload_state, UploadState::Ready);
    assert_eq!(
        lookup_object_ids(&remote, remote_file_after.ino, remote_attr_after.size)
            .expect("remote object ids after publish"),
        vec![block0.object_id, block1.object_id]
    );
}

#[test]
fn unit_remote_projection_converges_nested_rename_overwrite_to_source_inode() {
    let (_writer_dir, writer) = temp_pg_engine_for_unit();
    let (_remote_dir, remote) = temp_pg_engine_for_unit();
    let block_size = DEFAULT_BLOCK_SIZE as u64;

    let seed_start = writer
        .local
        .get_counter("nextEventSeq")
        .expect("seed start");
    let dir = writer
        .create_dir(ROOT_INO, b"shared", 0, 0, 0o755)
        .expect("create dir");
    let old = writer
        .create_file(dir.ino, b"final.bin", 0, 0, 0o644)
        .expect("create old final");
    set_file_state(&writer, old.ino, 4096, 1);
    let old_block = PayloadRef::new_partial(901, 4096);
    writer
        .stage_payload_slice(old.ino, 0, &old_block)
        .expect("set old block");
    mark_overlay_objects_uploaded(&writer, old.ino, 1, &[old_block.object_id]);
    assert!(
        writer
            .publish_regular_file_overlay_to_base_if_matches(old.ino, 1)
            .expect("publish old base graph"),
        "old base graph publish must succeed"
    );
    let seed_end = writer.local.get_counter("nextEventSeq").expect("seed end");

    apply_projected_remote_range(&writer, &remote, seed_start, seed_end);

    let remote_dir = remote.lookup(ROOT_INO, b"shared").expect("lookup dir");
    let remote_old = remote
        .lookup(remote_dir.ino, b"final.bin")
        .expect("lookup seeded final");
    assert_eq!(remote_old.ino, old.ino);

    let replace_start = seed_end;
    let temp = writer
        .create_file(dir.ino, b"temp.bin", 0, 0, 0o644)
        .expect("create replacement temp");
    set_file_state(&writer, temp.ino, block_size + 4096, 2);
    let new_block0 = PayloadRef::new_full(902, DEFAULT_BLOCK_SIZE);
    let new_block1 = PayloadRef::new_partial(903, 4096);
    writer
        .stage_payload_slice(temp.ino, 0, &new_block0)
        .expect("set replacement payload slice 0");
    writer
        .stage_payload_slice(temp.ino, 1, &new_block1)
        .expect("set replacement payload slice 1");
    mark_overlay_objects_uploaded(
        &writer,
        temp.ino,
        2,
        &[new_block0.object_id, new_block1.object_id],
    );
    assert!(
        writer
            .publish_regular_file_overlay_to_base_if_matches(temp.ino, 2)
            .expect("publish replacement temp"),
        "replacement temp publish must succeed"
    );
    writer
        .rename(dir.ino, b"temp.bin", dir.ino, b"final.bin")
        .expect("rename replacement over final");
    let replace_end = writer
        .local
        .get_counter("nextEventSeq")
        .expect("replace end");

    apply_projected_remote_range(&writer, &remote, replace_start, replace_end);

    let remote_final = remote
        .lookup(remote_dir.ino, b"final.bin")
        .expect("lookup replaced final");
    assert_eq!(remote_final.ino, temp.ino);
    assert!(
        remote.get_attr(old.ino).is_err(),
        "old inode must be removed"
    );
    let remote_attr = remote
        .get_attr(remote_final.ino)
        .expect("get replaced attr");
    assert_eq!(remote_attr.size, block_size + 4096);
    assert_eq!(remote_attr.committed_size, block_size + 4096);
    assert_eq!(remote_attr.committed_content_epoch, 2);
    assert_eq!(remote_attr.upload_state, UploadState::Ready);
    assert_eq!(
        lookup_object_ids(&remote, remote_final.ino, remote_attr.size)
            .expect("replacement object ids"),
        vec![new_block0.object_id, new_block1.object_id]
    );
}

#[test]
fn unit_remote_projection_keeps_published_file_visible_when_unpublished_epoch_is_deleted() {
    let (_writer_dir, writer) = temp_pg_engine_for_unit();
    let (_remote_dir, remote) = temp_pg_engine_for_unit();

    let seed_start = writer
        .local
        .get_counter("nextEventSeq")
        .expect("seed start");
    let dir = writer
        .create_dir(ROOT_INO, b"shared", 0, 0, 0o755)
        .expect("create dir");
    let file = writer
        .create_file(dir.ino, b"final.bin", 0, 0, 0o644)
        .expect("create final");
    set_file_state(&writer, file.ino, 4096, 1);
    let published = PayloadRef::new_partial(904, 4096);
    writer
        .stage_payload_slice(file.ino, 0, &published)
        .expect("set published payload slice");
    mark_overlay_objects_uploaded(&writer, file.ino, 1, &[published.object_id]);
    assert!(
        writer
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish seed base graph"),
        "seed publish must succeed"
    );
    let seed_end = writer.local.get_counter("nextEventSeq").expect("seed end");

    apply_projected_remote_range(&writer, &remote, seed_start, seed_end);

    let remote_dir = remote.lookup(ROOT_INO, b"shared").expect("lookup dir");
    let remote_file = remote
        .lookup(remote_dir.ino, b"final.bin")
        .expect("lookup published file");
    assert_eq!(remote_file.ino, file.ino);

    let upload_start = seed_end;
    set_file_state(&writer, file.ino, 4096, 2);
    let uploaded_overlay = PayloadRef::new_partial(905, 4096);
    writer
        .stage_payload_slice(file.ino, 0, &uploaded_overlay)
        .expect("set unpublished overlay");
    assert!(
        writer
            .mark_regular_file_overlay_object_uploaded_if_matches(
                file.ino,
                2,
                uploaded_overlay.object_id,
            )
            .expect("mark overlay uploaded"),
        "overlay upload marker must succeed"
    );
    let upload_end = writer
        .local
        .get_counter("nextEventSeq")
        .expect("upload end");

    apply_projected_remote_range(&writer, &remote, upload_start, upload_end);

    writer
        .unlink(dir.ino, b"final.bin")
        .expect("unlink unpublished replacement");
    let delete_end = writer
        .local
        .get_counter("nextEventSeq")
        .expect("delete end");

    apply_projected_remote_range(&writer, &remote, upload_end, delete_end);

    let remote_after = remote
        .lookup(remote_dir.ino, b"final.bin")
        .expect("remote must retain last published entry");
    assert_eq!(remote_after.ino, file.ino);
    let remote_attr = remote.get_attr(file.ino).expect("remote attr");
    assert_eq!(remote_attr.size, 4096);
    assert_eq!(remote_attr.committed_size, 4096);
    assert_eq!(remote_attr.content_epoch, 1);
    assert_eq!(remote_attr.committed_content_epoch, 1);
    assert_eq!(
        lookup_object_ids(&remote, file.ino, remote_attr.size).expect("remote object ids"),
        vec![published.object_id]
    );

    assert!(
        writer.get_attr(file.ino).is_err(),
        "local unlink removes inode attr"
    );
    let lifecycle = writer
        .get_inode_lifecycle(file.ino)
        .expect("local lifecycle")
        .expect("deleted-before-publish lifecycle");
    assert_eq!(lifecycle.state, InodeLifecycleState::DeletedBeforePublish);
    assert!(lifecycle.has_published_base);
    assert_eq!(lifecycle.deferred_cleanup_object_ids, vec![905]);
    let pending_deletions = writer.get_pending_deletions().expect("pending deletions");
    assert!(
        pending_deletions
            .iter()
            .any(|(object_id, _timestamp)| *object_id == 905),
        "uploaded unpublished object should be queued for GC"
    );
    assert!(
        pending_deletions
            .iter()
            .all(|(object_id, _timestamp)| *object_id != 904),
        "last published object must remain referenced"
    );
}

#[test]
fn unit_remote_projection_preserves_overlay_publish_path() {
    let (_writer_dir, writer) = temp_pg_engine_for_unit();
    let (_remote_dir, remote) = temp_pg_engine_for_unit();

    let start = writer.local.get_counter("nextEventSeq").expect("start seq");
    let file = writer
        .create_file(ROOT_INO, b"overlay.bin", 0, 0, 0o644)
        .expect("create overlay file");
    set_file_state(&writer, file.ino, 4096, 1);
    let block = PayloadRef::new_partial(906, 4096);
    writer
        .stage_payload_slice(file.ino, 0, &block)
        .expect("set overlay block");
    assert!(
        writer
            .mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 1, block.object_id)
            .expect("mark overlay uploaded"),
        "uploaded marker must succeed"
    );
    assert!(
        writer
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish overlay to base"),
        "overlay publish must succeed"
    );
    let end = writer.local.get_counter("nextEventSeq").expect("end seq");

    apply_projected_remote_range(&writer, &remote, start, end);

    let remote_file = remote
        .lookup(ROOT_INO, b"overlay.bin")
        .expect("lookup overlay file");
    let remote_attr = remote.get_attr(remote_file.ino).expect("remote attr");
    assert_eq!(remote_attr.size, 4096);
    assert_eq!(remote_attr.committed_size, 4096);
    assert_eq!(remote_attr.committed_content_epoch, 1);
    assert_eq!(remote_attr.upload_state, UploadState::Ready);
    assert_eq!(
        lookup_object_ids(&remote, remote_file.ino, remote_attr.size)
            .expect("remote overlay object ids"),
        vec![block.object_id]
    );
}

#[test]
fn claim_and_restore_pending_batch_updates_replication_backlog() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        state.ops = 4;
        state.force_flush = true;
        state.event_start = Some(10);
        state.event_end = 14;
        state.started_at = Some(Instant::now());
        state.last_mutation_at = Some(Instant::now());
    }

    let batch = pg.claim_pending_batch(true).expect("claimed batch");
    let claimed = pg.replication_backlog();
    assert_eq!(claimed.pending_ops, 0);
    assert_eq!(claimed.inflight_ops, 4);

    pg.restore_pending_batch(batch);
    let restored = pg.replication_backlog();
    assert_eq!(restored.pending_ops, 4);
    assert_eq!(restored.inflight_ops, 0);
}

#[test]
fn restore_pending_state_from_local_log_rebuilds_unpublished_range() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let start = pg.local.get_counter("nextEventSeq").expect("baseline seq");

    pg.local
        .create_dir(ROOT_INO, b"draft", 0, 0, 0o755)
        .expect("create draft dir");
    pg.local
        .create_file(ROOT_INO, b"draft.txt", 0, 0, 0o644)
        .expect("create draft file");

    {
        let mut state = pg.pending_state.lock().expect("pending_state");
        *state = PendingReplicationState::default();
    }

    pg.restore_pending_state_from_local_log()
        .expect("restore pending state");

    let end = pg.local.get_counter("nextEventSeq").expect("end seq");
    let backlog = pg.replication_backlog();
    assert_eq!(backlog.pending_ops, (end - start) as u32);
    let state = pg.pending_state.lock().expect("pending_state");
    assert!(state.force_flush);
    assert_eq!(state.event_start, Some(start));
    assert_eq!(state.event_end, end);
}

#[test]
fn restore_pending_state_from_local_log_replays_unflushed_serving_state() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"lww-winner.txt", 0, 0, 0o644)
        .expect("create file");
    let setup_start = pg.flushed_event_seq.load(Ordering::SeqCst);
    let setup_end = pg.local.get_counter("nextEventSeq").expect("setup end");
    let setup_events = pg
        .local
        .read_events_range(setup_start, setup_end)
        .expect("read setup events");
    pg.local
        .apply_replica_events_to_base(&setup_events)
        .expect("publish setup to local base");
    pg.persist_flushed_event_seq(setup_end)
        .expect("mark setup flushed");

    let mut remote_empty = pg.get_attr(file.ino).expect("remote empty attr");
    remote_empty.size = 0;
    remote_empty.committed_size = 0;
    remote_empty.content_epoch = 2;
    remote_empty.committed_content_epoch = 2;
    remote_empty.upload_state = UploadState::Ready;
    pg.local
        .apply_replica_events_to_base(&[MetaEvent::SetAttr {
            attr: remote_empty.clone(),
        }])
        .expect("apply empty remote base attr");
    pg.local
        .apply_replica_serving_state(&[MetaEvent::SetAttr {
            attr: remote_empty.clone(),
        }])
        .expect("apply empty remote serving attr");

    let winner_payload = PayloadRef::new_partial(150_003, 271);
    pg.stage_payload_slice(file.ino, 0, &winner_payload)
        .expect("stage local winner payload");
    let mut local_winner = remote_empty.clone();
    local_winner.size = 271;
    local_winner.content_epoch = 3;
    local_winner.committed_content_epoch = 2;
    local_winner.upload_state = UploadState::Updating;
    pg.set_attr(&local_winner).expect("set local winner attr");
    assert_eq!(pg.get_attr(file.ino).expect("winner visible").size, 271);

    let mut reconcile = HashSet::new();
    reconcile.insert(file.ino);
    pg.local
        .reconcile_regular_file_serving_from_base(&reconcile)
        .expect("simulate startup remote reconcile over serving state");
    assert_eq!(
        pg.get_attr(file.ino).expect("remote attr visible").size,
        0,
        "remote catch-up can leave serving state stale before pending replay"
    );

    pg.restore_pending_state_from_local_log()
        .expect("restore pending local log");
    let restored = pg.get_attr(file.ino).expect("restored winner attr");
    assert_eq!(restored.size, local_winner.size);
    assert_eq!(restored.content_epoch, local_winner.content_epoch);
    let spans = pg
        .lookup_regular_file_slices(file.ino, 0, local_winner.size)
        .expect("restored winner slices");
    assert!(
        spans
            .iter()
            .any(|span| span.payload.as_ref().is_some_and(|payload| {
                payload.object_id == winner_payload.object_id
                    && payload.length == winner_payload.length
            })),
        "pending log replay must restore the unflushed local payload overlay"
    );
}

#[test]
fn pg_event_insert_chunking_covers_all_events() {
    let events = vec![MetaEvent::RemoveAttr { ino: 1 }; 1_025];
    let chunk_sizes: Vec<usize> = events
        .chunks(PG_EVENT_INSERT_CHUNK_SIZE)
        .map(|chunk| chunk.len())
        .collect();

    assert_eq!(chunk_sizes, vec![512, 512, 1]);
}

#[test]
fn metadata_notify_channel_is_distinct_from_lock_notify_channel() {
    assert_eq!(
        PgMetaEngine::metadata_notify_channel(7),
        "studiofs_meta_vol_7"
    );
    assert_ne!(
        PgMetaEngine::metadata_notify_channel(7),
        PgMetaEngine::lock_notify_channel(7),
        "metadata invalidation and lock lease wakeups must stay on separate PG channels"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_flush_advances_checkpoint_version() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = tempdir().expect("tempdir");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-test");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    pg.create_file(ROOT_INO, b"a.txt", 0, 0, 0o644)
        .expect("create file");
    pg.flush_metadata().expect("flush metadata");
    let ok = wait_until(Duration::from_secs(10), || {
        run_async(async {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let conn_task = tokio::spawn(async move {
                let _ = connection.await;
            });
            let row = client
                .query_opt(
                    "SELECT version FROM metadata_state WHERE vol=$1",
                    &[&volume.volume_id],
                )
                .await
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            conn_task.abort();
            Ok::<bool, MetaError>(row.map(|r| r.get::<_, i64>(0) >= 1).unwrap_or(false))
        })
        .unwrap_or(false)
    });
    assert!(ok, "expected metadata_state version >= 1");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_clients_reserve_disjoint_local_id_ranges() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-client-slots");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let a_slot = PgMetaEngine::load_persisted_client_slot(&a.local)
        .expect("read a slot")
        .expect("a slot");
    let b_slot = PgMetaEngine::load_persisted_client_slot(&b.local)
        .expect("read b slot")
        .expect("b slot");
    assert_ne!(
        a_slot, b_slot,
        "distinct clients must reserve distinct slots"
    );

    let a_next_inode = a
        .local
        .get_counter(counters::NEXT_INODE)
        .expect("a next inode");
    let b_next_inode = b
        .local
        .get_counter(counters::NEXT_INODE)
        .expect("b next inode");
    assert_ne!(a_next_inode, b_next_inode);

    let a_file = a
        .create_file(ROOT_INO, b"a.txt", 0, 0, 0o644)
        .expect("a create file");
    let b_file = b
        .create_file(ROOT_INO, b"b.txt", 0, 0, 0o644)
        .expect("b create file");
    assert_ne!(
        a_file.ino, b_file.ino,
        "new inode allocations must not collide"
    );
    let a_objects = a.next_object_ids(2).expect("a object ids");
    let b_objects = b.next_object_ids(2).expect("b object ids");
    assert!(
        a_objects.iter().all(|id| !b_objects.contains(id)),
        "new object ID allocations must not collide"
    );
    assert_eq!(a_objects, vec![a_file.ino + 1, a_file.ino + 2]);
    assert_eq!(b_objects, vec![b_file.ino + 1, b_file.ino + 2]);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_inode_windows_are_scoped_per_volume() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume_a = PgTestVolume::new(&dsn, "pg-inode-window-volume-a");
    let volume_b = PgTestVolume::new(&dsn, "pg-inode-window-volume-b");
    seed_local(&db_a, &volume_a.format_info());
    seed_local(&db_b, &volume_b.format_info());

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let a_window = load_inode_state(&a.local);
    let b_window = load_inode_state(&b.local);
    assert_eq!(
        a_window.current.start,
        ROOT_INO + 1,
        "first inode window for a new volume should start at the root successor"
    );
    assert_eq!(
        b_window.current.start,
        ROOT_INO + 1,
        "inode windows must be scoped per volume rather than globally across postgres"
    );

    let a_file = a
        .create_file(ROOT_INO, b"a-volume.txt", 0, 0, 0o644)
        .expect("a create file");
    let b_file = b
        .create_file(ROOT_INO, b"b-volume.txt", 0, 0, 0o644)
        .expect("b create file");
    assert_eq!(
        a_file.ino, b_file.ino,
        "different volumes may reuse inode values because each volume owns its own namespace"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_sync_metadata_between_clients() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-sync");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    a.create_dir(ROOT_INO, b"shared", 0, 0, 0o755)
        .expect("create dir on a");
    a.flush_metadata().expect("flush a");
    assert!(
        wait_visible(&b, ROOT_INO, b"shared", Duration::from_secs(10)),
        "shared dir should become visible on b"
    );
    let entry = b.lookup(ROOT_INO, b"shared").expect("lookup on b");
    assert!(entry.ino > 0);
    assert_eq!(entry.kind, FileType::Directory);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_metadata_notify_listener_wakes_remote_sync_without_poll() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let _poll_guard = set_test_env("STUDIOFS_PG_SYNC_POLL_SECS", "3600");

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-metadata-notify-sync");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let channel = PgMetaEngine::metadata_notify_channel(volume.volume_id);
    let readiness_received = wait_until(Duration::from_secs(10), || {
        if b.metadata_invalidation_diagnostics().notifications_received > 0 {
            true
        } else {
            send_pg_notify(&dsn, &channel, "ready");
            false
        }
    });
    let readiness_diagnostics = b.metadata_invalidation_diagnostics();
    let (readiness_listener_started, readiness_listener_finished) = b
        .metadata_notify_handle
        .lock()
        .expect("metadata notify handle")
        .as_ref()
        .map(|handle| (true, handle.is_finished()))
        .unwrap_or((false, false));
    assert!(
        readiness_received,
        "metadata notify listener should receive a readiness notification before the metadata assertion; diagnostics={readiness_diagnostics:?}; listener_started={readiness_listener_started}; listener_finished={readiness_listener_finished}"
    );

    a.create_dir(ROOT_INO, b"notify-sync", 0, 0, 0o755)
        .expect("create dir on a");
    a.flush_metadata().expect("flush a");

    let visible = wait_until(Duration::from_secs(10), || {
        b.lookup(ROOT_INO, b"notify-sync").is_ok()
    });
    let diagnostics = b.metadata_invalidation_diagnostics();
    let (listener_started, listener_finished) = b
        .metadata_notify_handle
        .lock()
        .expect("metadata notify handle")
        .as_ref()
        .map(|handle| (true, handle.is_finished()))
        .unwrap_or((false, false));
    assert!(
        visible,
        "metadata NOTIFY should wake the remote peer without waiting for the long fallback poll; diagnostics={diagnostics:?}; listener_started={listener_started}; listener_finished={listener_finished}"
    );
    assert!(
        diagnostics.notifications_received > 0,
        "peer should count the metadata notification that woke sync"
    );
    assert!(
        diagnostics.last_successful_sync_unix_ms.is_some(),
        "peer should record the successful NOTIFY-driven metadata sync"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_metadata_notify_listener_start_syncs_missed_change() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let _poll_guard = set_test_env("STUDIOFS_PG_SYNC_POLL_SECS", "3600");

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-metadata-notify-missed");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open_with_options(
        &db_b,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open b without listener");

    a.create_dir(ROOT_INO, b"missed-notify", 0, 0, 0o755)
        .expect("create dir on a");
    a.flush_metadata().expect("flush a");
    assert!(
        b.lookup(ROOT_INO, b"missed-notify").is_err(),
        "peer with listener disabled should not see the change before a sync"
    );

    b.ensure_metadata_notify_listener_started()
        .expect("start metadata notify listener");

    assert!(
        wait_until(Duration::from_secs(10), || b
            .lookup(ROOT_INO, b"missed-notify")
            .is_ok()),
        "listener startup should force an immediate sync for missed notifications"
    );
    assert!(
        wait_until(Duration::from_secs(10), || b
            .metadata_invalidation_diagnostics()
            .last_successful_sync_unix_ms
            .is_some()),
        "forced reconnect/start sync should update metadata sync diagnostics; diagnostics={:?}",
        b.metadata_invalidation_diagnostics()
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_metadata_notify_listener_releases_engine_after_drop() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let _poll_guard = set_test_env("STUDIOFS_PG_SYNC_POLL_SECS", "3600");

    let dir = tempdir().expect("tempdir");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-metadata-notify-drop");
    let info = volume.format_info();
    seed_local(&db, &info);

    let engine = PgMetaEngine::open(&db).expect("open engine");
    let channel = PgMetaEngine::metadata_notify_channel(volume.volume_id);
    assert!(
        wait_until(Duration::from_secs(10), || {
            if engine
                .metadata_invalidation_diagnostics()
                .notifications_received
                > 0
            {
                true
            } else {
                send_pg_notify(&dsn, &channel, "ready");
                false
            }
        }),
        "metadata notify listener should receive a readiness notification"
    );

    let weak = Arc::downgrade(&engine);
    drop(engine);

    assert!(
        wait_until(Duration::from_secs(3), || weak.upgrade().is_none()),
        "metadata notify listener must not keep PgMetaEngine alive after the last external Arc drops"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_remote_client_reads_committed_blocks_without_local_staged_mirror() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-committed-block-read");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = create_published_small_file(&a, ROOT_INO, b"remote.bin", 50_301);
    set_file_state(&a, file.ino, 4096, 2);
    let block = PayloadRef::new_partial(77, 4096);
    a.stage_payload_slice(file.ino, 0, &block)
        .expect("set staged payload slice");
    mark_overlay_objects_uploaded(&a, file.ino, 2, &[block.object_id]);
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, 2)
            .expect("publish committed base graph")
    );
    a.flush_metadata().expect("flush a");

    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"remote.bin",
        Duration::from_secs(10)
    ));
    let remote = b.lookup(ROOT_INO, b"remote.bin").expect("lookup remote");
    assert_eq!(
        lookup_first_object_id(&b, remote.ino)
            .expect("get remote payload slice")
            .expect("remote committed payload slice"),
        77
    );

    assert_eq!(
        lookup_object_ids(&b, remote.ino, 4096).expect("get remote object ids"),
        vec![77]
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_remote_uploading_marker_replication() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-remote-uploading-marker");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = a
        .create_file(ROOT_INO, b"uploading.bin", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&a, file.ino, 4096, 1);
    a.stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(501, 4096))
        .expect("set staged payload slice");
    a.flush_metadata().expect("flush uploading marker");

    assert!(wait_until(Duration::from_secs(10), || {
        let _ = b.sync_metadata();
        matches!(
            b.lookup(ROOT_INO, b"uploading.bin"),
            Err(MetaError::EntryNotFound { .. })
        )
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_nested_base_graph_publish_visible_on_remote_peer() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-nested-base-graph-publish");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let dir = a
        .create_dir(ROOT_INO, b"nested", 0, 0, 0o755)
        .expect("create nested dir");
    a.flush_metadata().expect("flush nested dir");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"nested",
        Duration::from_secs(10)
    ));

    let file = a
        .create_file(dir.ino, b"leaf.bin", 0, 0, 0o644)
        .expect("create nested file");
    set_file_state(&a, file.ino, 4096, 1);
    let block = PayloadRef::new_partial(511, 4096);
    a.stage_payload_slice(file.ino, 0, &block)
        .expect("set nested block");
    mark_overlay_objects_uploaded(&a, file.ino, 1, &[block.object_id]);
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish nested base graph")
    );
    a.flush_metadata().expect("flush nested publish");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = b.sync_metadata();
        match b.lookup(ROOT_INO, b"nested") {
            Ok(remote_dir) => match b.lookup(remote_dir.ino, b"leaf.bin") {
                Ok(remote_file) => b
                    .get_attr(remote_file.ino)
                    .map(|attr| {
                        attr.size == 4096
                            && attr.committed_size == 4096
                            && attr.upload_state == UploadState::Ready
                    })
                    .unwrap_or(false),
                Err(_) => false,
            },
            Err(_) => false,
        }
    }));

    let remote_dir = b.lookup(ROOT_INO, b"nested").expect("lookup remote dir");
    let remote_file = b
        .lookup(remote_dir.ino, b"leaf.bin")
        .expect("lookup remote nested file");
    assert_eq!(
        lookup_object_ids(&b, remote_file.ino, 4096).expect("get remote nested object ids"),
        vec![block.object_id]
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_nested_temp_rename_then_publish_visible_on_remote_peer() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-nested-temp-rename-publish");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let dir = a
        .create_dir(ROOT_INO, b"drafts", 0, 0, 0o755)
        .expect("create drafts dir");
    a.flush_metadata().expect("flush drafts dir");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"drafts",
        Duration::from_secs(10)
    ));

    let file = a
        .create_file(dir.ino, b"temp.bin", 0, 0, 0o644)
        .expect("create temp file");
    a.rename(dir.ino, b"temp.bin", dir.ino, b"final.bin")
        .expect("rename temp to final");
    a.flush_metadata().expect("flush rename");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = b.sync_metadata();
        match b.lookup(ROOT_INO, b"drafts") {
            Ok(remote_dir) => matches!(
                b.lookup(remote_dir.ino, b"final.bin"),
                Err(MetaError::EntryNotFound { .. })
            ),
            Err(_) => false,
        }
    }));

    let block_size = DEFAULT_BLOCK_SIZE as u64;
    set_file_state(&a, file.ino, block_size + 4096, 1);
    let block0 = PayloadRef::new_full(611, DEFAULT_BLOCK_SIZE);
    let block1 = PayloadRef::new_partial(612, 4096);
    a.stage_payload_slice(file.ino, 0, &block0)
        .expect("set payload slice 0");
    a.stage_payload_slice(file.ino, 1, &block1)
        .expect("set payload slice 1");
    mark_overlay_objects_uploaded(&a, file.ino, 1, &[block0.object_id, block1.object_id]);
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish renamed nested base graph")
    );
    a.flush_metadata().expect("flush renamed nested base graph");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = b.sync_metadata();
        match b.lookup(ROOT_INO, b"drafts") {
            Ok(remote_dir) => match b.lookup(remote_dir.ino, b"final.bin") {
                Ok(remote_file) => {
                    let attr_ready = b
                        .get_attr(remote_file.ino)
                        .map(|attr| {
                            attr.size == block_size + 4096
                                && attr.committed_size == block_size + 4096
                                && attr.upload_state == UploadState::Ready
                        })
                        .unwrap_or(false);
                    let blocks_ready = lookup_object_ids(&b, remote_file.ino, block_size + 4096)
                        .map(|object_ids| object_ids == vec![block0.object_id, block1.object_id])
                        .unwrap_or(false);
                    attr_ready && blocks_ready
                }
                Err(_) => false,
            },
            Err(_) => false,
        }
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_remote_update_keeps_old_committed_version_until_switch() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-old-committed-until-switch");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = a
        .create_file(ROOT_INO, b"committed.bin", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&a, file.ino, 4096, 1);
    let baseline = PayloadRef::new_partial(701, 4096);
    a.stage_payload_slice(file.ino, 0, &baseline)
        .expect("set baseline block");
    mark_overlay_objects_uploaded(&a, file.ino, 1, &[baseline.object_id]);
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish baseline base graph")
    );
    a.flush_metadata().expect("flush baseline");

    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"committed.bin",
        Duration::from_secs(10)
    ));
    let remote = b.lookup(ROOT_INO, b"committed.bin").expect("lookup remote");
    assert_eq!(
        lookup_first_object_id(&b, remote.ino)
            .expect("get baseline remote block")
            .expect("baseline remote block"),
        701
    );

    set_file_state(&a, file.ino, 4096, 2);
    let replacement = PayloadRef::new_partial(702, 4096);
    a.stage_payload_slice(file.ino, 0, &replacement)
        .expect("set replacement payload slice");
    a.flush_metadata().expect("flush updating marker");

    assert!(wait_until(Duration::from_secs(10), || {
        let _ = b.sync_metadata();
        b.get_attr(remote.ino)
            .map(|attr| attr.upload_state == UploadState::Updating)
            .unwrap_or(false)
    }));

    let remote_attr = b
        .get_attr(remote.ino)
        .expect("get remote attr after update");
    assert_eq!(remote_attr.upload_state, UploadState::Updating);
    assert_eq!(
        lookup_first_object_id(&b, remote.ino)
            .expect("get remote block after update")
            .expect("remote block after update"),
        701,
        "remote peer must keep serving the previous committed version until publish"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_publish_switch_is_atomic_across_peers() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-atomic-publish-switch");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = a
        .create_file(ROOT_INO, b"atomic.bin", 0, 0, 0o644)
        .expect("create file");
    let block_size = DEFAULT_BLOCK_SIZE as u64;
    set_file_state(&a, file.ino, block_size + 4096, 1);
    let baseline0 = PayloadRef::new_full(801, DEFAULT_BLOCK_SIZE);
    let baseline1 = PayloadRef::new_partial(802, 4096);
    a.stage_payload_slice(file.ino, 0, &baseline0)
        .expect("set baseline block 0");
    a.stage_payload_slice(file.ino, 1, &baseline1)
        .expect("set baseline block 1");
    mark_overlay_objects_uploaded(&a, file.ino, 1, &[baseline0.object_id, baseline1.object_id]);
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish baseline base graph")
    );
    a.flush_metadata().expect("flush baseline base graph");

    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"atomic.bin",
        Duration::from_secs(10)
    ));
    let remote = b.lookup(ROOT_INO, b"atomic.bin").expect("lookup remote");

    set_file_state(&a, file.ino, block_size + 4096, 2);
    let replacement0 = PayloadRef::new_full(901, DEFAULT_BLOCK_SIZE);
    let replacement1 = PayloadRef::new_partial(902, 4096);
    a.stage_payload_slice(file.ino, 0, &replacement0)
        .expect("set replacement payload slice 0");
    a.stage_payload_slice(file.ino, 1, &replacement1)
        .expect("set replacement payload slice 1");
    a.flush_metadata().expect("flush updating state");

    assert!(wait_until(Duration::from_secs(10), || {
        let _ = b.sync_metadata();
        let object_ids = lookup_object_ids(&b, remote.ino, block_size + 4096).unwrap_or_default();
        object_ids == vec![801, 802]
            && b.get_attr(remote.ino)
                .map(|attr| attr.upload_state == UploadState::Updating)
                .unwrap_or(false)
    }));

    mark_overlay_objects_uploaded(
        &a,
        file.ino,
        2,
        &[replacement0.object_id, replacement1.object_id],
    );
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, 2)
            .expect("publish replacement base graph")
    );
    a.flush_metadata().expect("flush replacement base graph");

    assert!(wait_until(Duration::from_secs(10), || {
        let _ = b.sync_metadata();
        let object_ids = lookup_object_ids(&b, remote.ino, block_size + 4096).unwrap_or_default();
        object_ids == vec![901, 902]
            && b.get_attr(remote.ino)
                .map(|attr| attr.upload_state == UploadState::Ready)
                .unwrap_or(false)
    }));

    assert_eq!(
        lookup_object_ids(&b, remote.ino, block_size + 4096).expect("get remote object ids"),
        vec![901, 902]
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_foreign_events_do_not_block_local_accepted_reads() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let dir_c = tempdir().expect("tempdir c");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let db_c = dir_c.path().join("meta-c.redb");
    let volume = PgTestVolume::new(&dsn, "pg-foreign-does-not-flush-local");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);
    seed_local(&db_c, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");
    let c = PgMetaEngine::open(&db_c).expect("open c");

    let draft = b
        .create_file(ROOT_INO, b"draft.bin", 0, 0, 0o644)
        .expect("create local draft");
    set_file_state(&b, draft.ino, 4096, 1);
    b.stage_payload_slice(draft.ino, 0, &PayloadRef::new_partial(1001, 4096))
        .expect("set local draft block");

    a.create_dir(ROOT_INO, b"foreign", 0, 0, 0o755)
        .expect("create foreign dir");
    a.flush_metadata().expect("flush foreign dir");

    assert!(wait_until(Duration::from_secs(10), || {
        if let Ok(mut state) = b.pending_state.lock() {
            state.force_flush = false;
            state.last_mutation_at = Some(Instant::now());
        }
        let _ = b.sync_metadata();
        b.lookup(ROOT_INO, b"foreign").is_ok()
    }));

    let local_attr = b.get_attr(draft.ino).expect("get local draft attr");
    assert_eq!(local_attr.size, 4096);
    assert_eq!(local_attr.committed_size, 0);
    assert_eq!(local_attr.upload_state, UploadState::Uploading);
    assert_eq!(
        lookup_first_object_id(&b, draft.ino)
            .expect("get local draft block")
            .expect("local draft block"),
        1001
    );

    if let Ok(mut state) = b.pending_state.lock() {
        state.force_flush = false;
        state.last_mutation_at = Some(Instant::now());
    }
    let _ = c.sync_metadata();
    assert!(
        c.lookup(ROOT_INO, b"draft.bin").is_err(),
        "syncing foreign events on b must not flush its local accepted draft into PG"
    );
    assert!(
        c.lookup(ROOT_INO, b"foreign").is_ok(),
        "foreign namespace convergence should still work while b has local-only accepted state"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_overlay_allows_unrelated_remote_batches_to_apply() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-overlay-prefix");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let draft = a
        .create_file(ROOT_INO, b"draft.bin", 0, 0, 0o644)
        .expect("create draft");
    let mut draft_attr = a.get_attr(draft.ino).expect("get draft attr");
    draft_attr.size = 4096;
    a.set_attr(&draft_attr).expect("set draft size");
    a.stage_payload_slice(draft.ino, 0, &PayloadRef::new_partial(501, 4096))
        .expect("set draft staged payload slice");
    a.flush_metadata().expect("flush draft namespace");

    b.create_dir(ROOT_INO, b"foreign.txt", 0, 0, 0o755)
        .expect("create foreign");
    b.flush_metadata().expect("flush foreign");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = a.sync_metadata();
        a.lookup(ROOT_INO, b"foreign.txt").is_ok()
    }));

    let draft_after = a.get_attr(draft.ino).expect("get local draft");
    assert_eq!(draft_after.size, 4096);
    assert_eq!(draft_after.committed_size, 0);
    assert_eq!(
        lookup_first_object_id(&a, draft.ino)
            .expect("get staged draft block")
            .expect("staged draft block exists"),
        501
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_committed_block_applies_despite_local_attr_overlay() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-committed-block-vs-attr-overlay");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = a
        .create_file(ROOT_INO, b"remote.bin", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&a, file.ino, 4096, 1);
    a.stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(87, 4096))
        .expect("set baseline remote block");
    mark_overlay_objects_uploaded(&a, file.ino, 1, &[87]);
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish baseline base graph")
    );
    a.flush_metadata().expect("flush create");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"remote.bin",
        Duration::from_secs(10)
    ));

    let b_file = b.lookup(ROOT_INO, b"remote.bin").expect("lookup on b");
    let mut local_attr = b.get_attr(b_file.ino).expect("get local attr");
    local_attr.size = 8192;
    b.set_attr(&local_attr).expect("set local attr overlay");

    set_file_state(&a, file.ino, 4096, 2);
    let block = PayloadRef::new_partial(88, 4096);
    a.stage_payload_slice(file.ino, 0, &block)
        .expect("set staged remote slice");
    mark_overlay_objects_uploaded(&a, file.ino, 2, &[block.object_id]);
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, 2)
            .expect("publish remote slice graph")
    );
    a.flush_metadata().expect("flush remote slice graph");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = b.sync_metadata();
        lookup_first_object_id(&b, b_file.ino)
            .map(|object_id| object_id == Some(88))
            .unwrap_or(false)
    }));

    assert_eq!(
        lookup_first_object_id(&b, b_file.ino)
            .expect("get committed payload slice on b")
            .expect("committed payload slice visible on b"),
        88
    );

    let attr_after = b.get_attr(b_file.ino).expect("get local attr after sync");
    assert_eq!(
        attr_after.size, 8192,
        "local attr overlay should still mask remote size"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_conflicting_batch_does_not_block_later_foreign_batch() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = tempdir().expect("tempdir");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-conflict-gap");
    let info = volume.format_info();
    seed_local(&db, &info);

    let pg = PgMetaEngine::open(&db).expect("open pg");
    let draft = pg
        .create_file(ROOT_INO, b"draft.bin", 0, 0, 0o644)
        .expect("create draft");
    let mut draft_attr = pg.get_attr(draft.ino).expect("get draft attr");
    draft_attr.size = 4096;
    pg.set_attr(&draft_attr).expect("set staged size");
    pg.stage_payload_slice(draft.ino, 0, &PayloadRef::new_partial(601, 4096))
        .expect("set draft block");

    let mut conflicted_attr = draft_attr.clone();
    conflicted_attr.size = 8192;

    let remote_ino = 1_234_567_890u64;
    let remote_attr = InodeAttr::new_dir(remote_ino, ROOT_INO, 0, 0, 0o755);

    let conflict_event = MetaEvent::SetAttr {
        attr: conflicted_attr,
    };
    let add_event = MetaEvent::SetAttr {
        attr: remote_attr.clone(),
    };
    let dentry_event = MetaEvent::AddEntry {
        parent: ROOT_INO,
        name: b"unrelated.txt".to_vec(),
        entry: DirEntryValue {
            ino: remote_ino,
            kind: FileType::Directory,
        },
    };

    let conflict_payload = postcard::to_stdvec(&conflict_event).expect("encode conflict event");
    let add_payload = postcard::to_stdvec(&add_event).expect("encode add attr event");
    let dentry_payload = postcard::to_stdvec(&dentry_event).expect("encode unrelated dentry event");

    run_async(async move {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });

        client
            .execute(
                "INSERT INTO event_log (vol, batch_version, event_type, event_payload, client_id) \
                    VALUES ($1, 1, $2, $3, 'manual-foreign')",
                &[&volume.volume_id, &conflict_event.kind(), &conflict_payload],
            )
            .await
            .map_err(|e| MetaError::Storage(format!("insert conflict event failed: {e}")))?;

        client
            .execute(
                "INSERT INTO event_log (vol, batch_version, event_type, event_payload, client_id) \
                    VALUES ($1, 2, $2, $3, 'manual-foreign')",
                &[&volume.volume_id, &add_event.kind(), &add_payload],
            )
            .await
            .map_err(|e| MetaError::Storage(format!("insert remote attr event failed: {e}")))?;
        client
            .execute(
                "INSERT INTO event_log (vol, batch_version, event_type, event_payload, client_id) \
                    VALUES ($1, 2, $2, $3, 'manual-foreign')",
                &[&volume.volume_id, &dentry_event.kind(), &dentry_payload],
            )
            .await
            .map_err(|e| MetaError::Storage(format!("insert remote dentry event failed: {e}")))?;
        conn_task.abort();
        Ok(())
    })
    .expect("write synthetic remote batches");

    pg.sync_metadata().expect("sync remote batches");
    let remote = pg
        .lookup(ROOT_INO, b"unrelated.txt")
        .expect("lookup unrelated entry");
    assert_eq!(remote.ino, remote_ino);

    let local = pg
        .get_attr(draft.ino)
        .expect("lookup local draft attr after sync");
    assert_eq!(
        local.size, 4096,
        "conflicting foreign batch should not clobber local overlay"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_checkpoint_excludes_uncommitted_local_payloads() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-projection");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let file = a
        .create_file(ROOT_INO, b"draft.bin", 0, 0, 0o644)
        .expect("create draft");

    let mut attr = a.get_attr(file.ino).expect("get attr");
    attr.size = 4096;
    attr.mtime = InodeAttr::now_nanos();
    attr.ctime = attr.mtime;
    a.set_attr(&attr).expect("set staged size");
    a.stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(42, 4096))
        .expect("set staged payload slice");
    a.flush_metadata().expect("flush a");

    let b = PgMetaEngine::open(&db_b).expect("open b");
    assert!(
        wait_hidden(&b, ROOT_INO, b"draft.bin", Duration::from_secs(10)),
        "checkpoint must keep a brand-new unpublished file hidden from peers"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_remote_unlink_removes_file_from_namespace_immediately() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-remote-unlink");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    create_published_small_file(&a, ROOT_INO, b"remote-delete.txt", 50_302);
    a.flush_metadata().expect("flush create");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"remote-delete.txt",
        Duration::from_secs(10)
    ));

    a.unlink(ROOT_INO, b"remote-delete.txt")
        .expect("unlink on a");
    a.flush_metadata().expect("flush unlink");

    assert!(wait_until(Duration::from_secs(10), || {
        let _ = b.sync_metadata();
        b.lookup(ROOT_INO, b"remote-delete.txt").is_err()
    }));
    assert!(b.lookup(ROOT_INO, b"remote-delete.txt").is_err());
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_peer_can_unlink_foreign_visible_uploading_entry() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-peer-unlink-uploading");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = a
        .create_file(ROOT_INO, b"uploading.bin", 0, 0, 0o644)
        .expect("create uploading file");
    set_file_state(&a, file.ino, 4096, 1);
    a.stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(776, 4096))
        .expect("set baseline uploading block");
    mark_overlay_objects_uploaded(&a, file.ino, 1, &[776]);
    assert!(
        a.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish baseline base graph")
    );
    a.flush_metadata().expect("flush baseline");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"uploading.bin",
        Duration::from_secs(10)
    ));

    set_file_state(&a, file.ino, 4096, 2);
    a.stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(777, 4096))
        .expect("set local uploading block");
    a.flush_metadata().expect("flush uploading namespace");

    assert!(wait_until(Duration::from_secs(10), || {
        let _ = b.sync_metadata();
        b.lookup(ROOT_INO, b"uploading.bin")
            .map(|entry| {
                b.get_attr(entry.ino)
                    .map(|attr| attr.upload_state == UploadState::Updating)
                    .unwrap_or(false)
            })
            .unwrap_or(false)
    }));

    b.unlink(ROOT_INO, b"uploading.bin")
        .expect("unlink uploading file on peer");
    b.flush_metadata().expect("flush peer unlink");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = a.sync_metadata();
        let _ = b.sync_metadata();
        a.lookup(ROOT_INO, b"uploading.bin").is_ok()
            && b.lookup(ROOT_INO, b"uploading.bin").is_err()
    }));
    let local_attr = a
        .get_attr(file.ino)
        .expect("get local attr after peer unlink");
    assert_eq!(local_attr.upload_state, UploadState::Ready);
    assert_eq!(
        lookup_first_object_id(&a, file.ino)
            .expect("get local payload after peer unlink")
            .expect("writer keeps local pending payload after peer unlink"),
        777
    );

    drop(a);
    let reopened = reopen_pg_engine_with_retry(&db_a);
    reopened.sync_metadata().expect("sync reopened a");
    assert!(reopened.lookup(ROOT_INO, b"uploading.bin").is_ok());
    assert_eq!(
        lookup_first_object_id(&reopened, file.ino)
            .expect("get reopened local payload")
            .expect("reopened local payload"),
        777
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_remote_rename_overwrite_converges_to_source_inode() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-rename-overwrite");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let src = create_published_small_file(&a, ROOT_INO, b"src.txt", 50_304);
    let dst = create_published_small_file(&a, ROOT_INO, b"dst.txt", 50_305);
    a.flush_metadata().expect("flush seed files");

    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"src.txt",
        Duration::from_secs(10)
    ));
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"dst.txt",
        Duration::from_secs(10)
    ));

    a.rename(ROOT_INO, b"src.txt", ROOT_INO, b"dst.txt")
        .expect("rename overwrite on a");
    a.flush_metadata().expect("flush rename overwrite");

    assert!(wait_until(Duration::from_secs(10), || {
        let _ = b.sync_metadata();
        b.lookup(ROOT_INO, b"src.txt").is_err()
            && b.lookup(ROOT_INO, b"dst.txt")
                .map(|entry| entry.ino == src.ino)
                .unwrap_or(false)
    }));

    let on_b = b.lookup(ROOT_INO, b"dst.txt").expect("lookup dst on b");
    assert_eq!(on_b.ino, src.ino);
    assert_ne!(on_b.ino, dst.ino);
    assert!(b.lookup(ROOT_INO, b"src.txt").is_err());
    assert!(
        b.get_attr(dst.ino).is_err(),
        "overwritten inode should be removed"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_peer_can_rename_foreign_visible_base_entry() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-peer-rename-base");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = create_published_small_file(&a, ROOT_INO, b"remote.txt", 50_306);
    a.flush_metadata().expect("flush create");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"remote.txt",
        Duration::from_secs(10)
    ));

    b.rename(ROOT_INO, b"remote.txt", ROOT_INO, b"renamed-by-b.txt")
        .expect("rename on b");
    b.flush_metadata().expect("flush rename on b");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = a.sync_metadata();
        a.lookup(ROOT_INO, b"remote.txt").is_err()
            && a.lookup(ROOT_INO, b"renamed-by-b.txt")
                .map(|entry| entry.ino == file.ino)
                .unwrap_or(false)
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_peer_can_unlink_foreign_visible_base_entry() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-peer-unlink-base");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    create_published_small_file(&a, ROOT_INO, b"remote.txt", 50_307);
    a.flush_metadata().expect("flush create");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"remote.txt",
        Duration::from_secs(10)
    ));

    b.unlink(ROOT_INO, b"remote.txt").expect("unlink on b");
    b.flush_metadata().expect("flush unlink on b");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = a.sync_metadata();
        a.lookup(ROOT_INO, b"remote.txt").is_err()
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_reopen_after_peer_unlink_base_recovers_without_discover() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-reopen-peer-unlink-base");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = create_published_small_file(&a, ROOT_INO, b"remote.txt", 50_308);
    a.flush_metadata().expect("flush create");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"remote.txt",
        Duration::from_secs(10)
    ));

    b.unlink(ROOT_INO, b"remote.txt").expect("unlink on b");
    b.flush_metadata().expect("flush unlink on b");
    assert!(wait_until(Duration::from_secs(12), || {
        let _ = a.sync_metadata();
        a.lookup(ROOT_INO, b"remote.txt").is_err()
    }));

    drop(a);
    let reopened_a = reopen_pg_engine_with_retry(&db_a);
    assert!(wait_until(Duration::from_secs(12), || {
        let _ = reopened_a.sync_metadata();
        reopened_a.lookup(ROOT_INO, b"remote.txt").is_err()
            && reopened_a.get_attr(file.ino).is_err()
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_own_flush_does_not_skip_earlier_foreign_batch() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-own-flush-gap");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    a.create_dir(ROOT_INO, b"from_a.txt", 0, 0, 0o755)
        .expect("create on a");
    a.flush_metadata().expect("flush a");

    b.create_dir(ROOT_INO, b"from_b.txt", 0, 0, 0o755)
        .expect("create on b");
    b.flush_metadata().expect("flush b");

    assert!(
        wait_visible(&b, ROOT_INO, b"from_a.txt", Duration::from_secs(12)),
        "client B must not skip an earlier foreign batch when it publishes its own batch"
    );
    assert!(
        wait_visible(&a, ROOT_INO, b"from_b.txt", Duration::from_secs(12)),
        "client A should observe client B's batch too"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_reopen_after_earlier_foreign_batch_gap_recovers_without_discover() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-reopen-gap");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    a.create_dir(ROOT_INO, b"from_a.txt", 0, 0, 0o755)
        .expect("create on a");
    a.flush_metadata().expect("flush a");

    b.create_dir(ROOT_INO, b"from_b.txt", 0, 0, 0o755)
        .expect("create on b");
    b.flush_metadata().expect("flush b");

    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"from_a.txt",
        Duration::from_secs(12)
    ));
    drop(b);

    let reopened_b = reopen_pg_engine_with_retry(&db_b);
    assert!(wait_until(Duration::from_secs(12), || {
        let _ = reopened_b.sync_metadata();
        reopened_b.lookup(ROOT_INO, b"from_a.txt").is_ok()
            && reopened_b.lookup(ROOT_INO, b"from_b.txt").is_ok()
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_two_clients_different_dirs_no_loss() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-dirs");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let dir_a_attr = a
        .create_dir(ROOT_INO, b"client_a", 0, 0, 0o755)
        .expect("a create dir");
    let dir_b_attr = b
        .create_dir(ROOT_INO, b"client_b", 0, 0, 0o755)
        .expect("b create dir");

    for i in 0..20 {
        let name = format!("a_{i:02}.txt");
        a.create_dir(dir_a_attr.ino, name.as_bytes(), 0, 0, 0o755)
            .expect("a create dir");
    }
    for i in 0..20 {
        let name = format!("b_{i:02}.txt");
        b.create_dir(dir_b_attr.ino, name.as_bytes(), 0, 0, 0o755)
            .expect("b create dir");
    }

    a.flush_metadata().expect("flush a");
    b.flush_metadata().expect("flush b");
    let converged = wait_until(Duration::from_secs(20), || {
        let _ = a.flush_metadata();
        let _ = b.flush_metadata();
        let _ = a.sync_metadata();
        let _ = b.sync_metadata();
        a.lookup(ROOT_INO, b"client_a").is_ok()
            && a.lookup(ROOT_INO, b"client_b").is_ok()
            && b.lookup(ROOT_INO, b"client_a").is_ok()
            && b.lookup(ROOT_INO, b"client_b").is_ok()
    });
    assert!(converged);

    let a_client_a = a.lookup(ROOT_INO, b"client_a").expect("a lookup client_a");
    let a_client_b = a.lookup(ROOT_INO, b"client_b").expect("a lookup client_b");
    let b_client_a = b.lookup(ROOT_INO, b"client_a").expect("b lookup client_a");
    let b_client_b = b.lookup(ROOT_INO, b"client_b").expect("b lookup client_b");

    let a_files_in_a = a.readdir(a_client_a.ino).expect("a readdir client_a");
    let a_files_in_b = a.readdir(a_client_b.ino).expect("a readdir client_b");
    let b_files_in_a = b.readdir(b_client_a.ino).expect("b readdir client_a");
    let b_files_in_b = b.readdir(b_client_b.ino).expect("b readdir client_b");

    assert_eq!(
        a_files_in_a.len(),
        20,
        "client A should see all files from A"
    );
    assert_eq!(
        a_files_in_b.len(),
        20,
        "client A should see all files from B"
    );
    assert_eq!(
        b_files_in_a.len(),
        20,
        "client B should see all files from A"
    );
    assert_eq!(
        b_files_in_b.len(),
        20,
        "client B should see all files from B"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_reopen_after_same_dir_concurrent_writes_recovers_without_discover() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-reopen-samedir");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let shared = a
        .create_dir(ROOT_INO, b"shared", 0, 0, 0o755)
        .expect("create shared dir");
    a.flush_metadata().expect("flush shared");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"shared",
        Duration::from_secs(10)
    ));

    for i in 0..20 {
        let name = format!("a_{i:02}.dat");
        a.create_dir(shared.ino, name.as_bytes(), 0, 0, 0o755)
            .expect("a create dir");
    }
    for i in 0..20 {
        let name = format!("b_{i:02}.dat");
        b.create_dir(shared.ino, name.as_bytes(), 0, 0, 0o755)
            .expect("b create dir");
    }

    a.flush_metadata().expect("flush a");
    b.flush_metadata().expect("flush b");
    assert!(wait_until(Duration::from_secs(20), || {
        let _ = a.flush_metadata();
        let _ = b.flush_metadata();
        let _ = a.sync_metadata();
        let _ = b.sync_metadata();
        a.readdir(shared.ino)
            .map(|v| v.len() == 40)
            .unwrap_or(false)
            && b.readdir(shared.ino)
                .map(|v| v.len() == 40)
                .unwrap_or(false)
    }));

    drop(a);
    drop(b);

    let reopened_a = reopen_pg_engine_with_retry(&db_a);
    let reopened_b = reopen_pg_engine_with_retry(&db_b);
    assert!(wait_until(Duration::from_secs(12), || {
        let _ = reopened_a.sync_metadata();
        let _ = reopened_b.sync_metadata();
        reopened_a
            .lookup(ROOT_INO, b"shared")
            .ok()
            .and_then(|dir| reopened_a.readdir(dir.ino).ok())
            .map(|v| v.len() == 40)
            .unwrap_or(false)
            && reopened_b
                .lookup(ROOT_INO, b"shared")
                .ok()
                .and_then(|dir| reopened_b.readdir(dir.ino).ok())
                .map(|v| v.len() == 40)
                .unwrap_or(false)
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_same_dir_different_files_no_loss() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-samedir");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let shared = a
        .create_dir(ROOT_INO, b"shared", 0, 0, 0o755)
        .expect("create shared dir");
    a.flush_metadata().expect("flush shared");
    assert!(
        wait_visible(&b, ROOT_INO, b"shared", Duration::from_secs(10)),
        "shared dir should become visible on b"
    );

    for i in 0..20 {
        let name = format!("a_{i:02}.dat");
        a.create_dir(shared.ino, name.as_bytes(), 0, 0, 0o755)
            .expect("a create dir");
    }
    for i in 0..20 {
        let name = format!("b_{i:02}.dat");
        b.create_dir(shared.ino, name.as_bytes(), 0, 0, 0o755)
            .expect("b create dir");
    }

    a.flush_metadata().expect("flush a");
    b.flush_metadata().expect("flush b");
    let converged = wait_until(Duration::from_secs(20), || {
        let _ = a.flush_metadata();
        let _ = b.flush_metadata();
        let _ = a.sync_metadata();
        let _ = b.sync_metadata();
        let a_ok = a
            .readdir(shared.ino)
            .map(|v| v.len() >= 40)
            .unwrap_or(false);
        let b_ok = b
            .readdir(shared.ino)
            .map(|v| v.len() >= 40)
            .unwrap_or(false);
        a_ok && b_ok
    });
    assert!(converged);

    let a_entries = a.readdir(shared.ino).expect("a readdir");
    let b_entries = b.readdir(shared.ino).expect("b readdir");
    assert_eq!(a_entries.len(), 40, "A should see all files in shared dir");
    assert_eq!(b_entries.len(), 40, "B should see all files in shared dir");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_concurrent_create_same_name_single_winner() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-concurrent-create");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let a2 = Arc::clone(&a);
    let b2 = Arc::clone(&b);
    let barrier_a = Arc::clone(&barrier);
    let barrier_b = Arc::clone(&barrier);

    let t1 = std::thread::spawn(move || {
        barrier_a.wait();
        let out = a2.create_dir(ROOT_INO, b"race.txt", 0, 0, 0o755);
        if out.is_ok() {
            let _ = a2.flush_metadata();
        }
        out
    });
    let t2 = std::thread::spawn(move || {
        barrier_b.wait();
        let out = b2.create_dir(ROOT_INO, b"race.txt", 0, 0, 0o755);
        if out.is_ok() {
            let _ = b2.flush_metadata();
        }
        out
    });

    let r1 = t1.join().expect("thread a");
    let r2 = t2.join().expect("thread b");

    let ok_count = [r1.is_ok(), r2.is_ok()].into_iter().filter(|v| *v).count();
    assert!(
        ok_count >= 1,
        "at least one client should create race.txt in async sync model"
    );

    a.flush_metadata().expect("flush a");
    b.flush_metadata().expect("flush b");
    assert!(wait_visible(
        &a,
        ROOT_INO,
        b"race.txt",
        Duration::from_secs(12)
    ));
    let entry = a.lookup(ROOT_INO, b"race.txt").expect("lookup race");
    assert!(entry.ino > 0);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_concurrent_file_vs_dir_same_name_single_winner() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-file-vs-dir-race");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let a2 = Arc::clone(&a);
    let b2 = Arc::clone(&b);
    let barrier_a = Arc::clone(&barrier);
    let barrier_b = Arc::clone(&barrier);

    let t1 = std::thread::spawn(move || {
        barrier_a.wait();
        let out = a2.create_file(ROOT_INO, b"victim", 0, 0, 0o644);
        if out.is_ok() {
            let _ = a2.flush_metadata();
        }
        out.map(|attr| (attr.ino, FileType::RegularFile))
    });
    let t2 = std::thread::spawn(move || {
        barrier_b.wait();
        let out = b2.create_dir(ROOT_INO, b"victim", 0, 0, 0o755);
        if out.is_ok() {
            let _ = b2.flush_metadata();
        }
        out.map(|attr| (attr.ino, FileType::Directory))
    });

    let r1 = t1.join().expect("thread a");
    let r2 = t2.join().expect("thread b");
    assert!(
        r1.is_ok() || r2.is_ok(),
        "at least one side must win the namespace race"
    );

    if let Ok(entry) = a.lookup(ROOT_INO, b"victim") {
        if a.get_attr(entry.ino).map(|attr| attr.kind).ok() == Some(FileType::RegularFile) {
            set_file_state(&a, entry.ino, 64, 1);
            let block = PayloadRef::new_partial(50_320, 64);
            a.stage_payload_slice(entry.ino, 0, &block)
                .expect("set winner block on a");
            mark_overlay_objects_uploaded(&a, entry.ino, 1, &[block.object_id]);
            assert!(
                a.publish_regular_file_overlay_to_base_if_matches(entry.ino, 1)
                    .expect("publish winner on a")
            );
            a.flush_metadata().expect("flush winner on a");
        }
    } else if let Ok(entry) = b.lookup(ROOT_INO, b"victim") {
        if b.get_attr(entry.ino).map(|attr| attr.kind).ok() == Some(FileType::RegularFile) {
            set_file_state(&b, entry.ino, 64, 1);
            let block = PayloadRef::new_partial(50_321, 64);
            b.stage_payload_slice(entry.ino, 0, &block)
                .expect("set winner block on b");
            mark_overlay_objects_uploaded(&b, entry.ino, 1, &[block.object_id]);
            assert!(
                b.publish_regular_file_overlay_to_base_if_matches(entry.ino, 1)
                    .expect("publish winner on b")
            );
            b.flush_metadata().expect("flush winner on b");
        }
    }

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = a.flush_metadata();
        let _ = b.flush_metadata();
        let _ = a.sync_metadata();
        let _ = b.sync_metadata();
        match (a.lookup(ROOT_INO, b"victim"), b.lookup(ROOT_INO, b"victim")) {
            (Ok(ae), Ok(be)) => match (a.get_attr(ae.ino), b.get_attr(be.ino)) {
                (Ok(a_attr), Ok(b_attr)) => ae.ino == be.ino && a_attr.kind == b_attr.kind,
                _ => false,
            },
            _ => false,
        }
    }));

    let a_entry = a.lookup(ROOT_INO, b"victim").expect("a lookup victim");
    let b_entry = b.lookup(ROOT_INO, b"victim").expect("b lookup victim");
    let a_attr = a.get_attr(a_entry.ino).expect("a attr victim");
    let b_attr = b.get_attr(b_entry.ino).expect("b attr victim");
    assert_eq!(a_entry.ino, b_entry.ino);
    assert_eq!(a_attr.kind, b_attr.kind);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_reopen_after_file_dir_race_recovers_without_discover() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-reopen-file-dir-race");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let a2 = Arc::clone(&a);
    let b2 = Arc::clone(&b);
    let barrier_a = Arc::clone(&barrier);
    let barrier_b = Arc::clone(&barrier);

    let t1 = std::thread::spawn(move || {
        barrier_a.wait();
        let out = a2.create_file(ROOT_INO, b"victim", 0, 0, 0o644);
        if out.is_ok() {
            let _ = a2.flush_metadata();
        }
        out
    });
    let t2 = std::thread::spawn(move || {
        barrier_b.wait();
        let out = b2.create_dir(ROOT_INO, b"victim", 0, 0, 0o755);
        if out.is_ok() {
            let _ = b2.flush_metadata();
        }
        out
    });

    let _ = t1.join().expect("thread a");
    let _ = t2.join().expect("thread b");

    if let Ok(entry) = a.lookup(ROOT_INO, b"victim") {
        if a.get_attr(entry.ino).map(|attr| attr.kind).ok() == Some(FileType::RegularFile) {
            set_file_state(&a, entry.ino, 64, 1);
            let block = PayloadRef::new_partial(50_322, 64);
            a.stage_payload_slice(entry.ino, 0, &block)
                .expect("set winner block on a");
            mark_overlay_objects_uploaded(&a, entry.ino, 1, &[block.object_id]);
            assert!(
                a.publish_regular_file_overlay_to_base_if_matches(entry.ino, 1)
                    .expect("publish winner on a")
            );
            a.flush_metadata().expect("flush winner on a");
        }
    } else if let Ok(entry) = b.lookup(ROOT_INO, b"victim") {
        if b.get_attr(entry.ino).map(|attr| attr.kind).ok() == Some(FileType::RegularFile) {
            set_file_state(&b, entry.ino, 64, 1);
            let block = PayloadRef::new_partial(50_323, 64);
            b.stage_payload_slice(entry.ino, 0, &block)
                .expect("set winner block on b");
            mark_overlay_objects_uploaded(&b, entry.ino, 1, &[block.object_id]);
            assert!(
                b.publish_regular_file_overlay_to_base_if_matches(entry.ino, 1)
                    .expect("publish winner on b")
            );
            b.flush_metadata().expect("flush winner on b");
        }
    }

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = a.flush_metadata();
        let _ = b.flush_metadata();
        let _ = a.sync_metadata();
        let _ = b.sync_metadata();
        match (a.lookup(ROOT_INO, b"victim"), b.lookup(ROOT_INO, b"victim")) {
            (Ok(ae), Ok(be)) => match (a.get_attr(ae.ino), b.get_attr(be.ino)) {
                (Ok(a_attr), Ok(b_attr)) => ae.ino == be.ino && a_attr.kind == b_attr.kind,
                _ => false,
            },
            _ => false,
        }
    }));

    let expected = a.lookup(ROOT_INO, b"victim").expect("winner on a");
    let expected_attr = a.get_attr(expected.ino).expect("winner attr on a");

    drop(a);
    drop(b);

    let reopened_a = reopen_pg_engine_with_retry(&db_a);
    let reopened_b = reopen_pg_engine_with_retry(&db_b);

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = reopened_a.sync_metadata();
        let _ = reopened_b.sync_metadata();
        match (
            reopened_a.lookup(ROOT_INO, b"victim"),
            reopened_b.lookup(ROOT_INO, b"victim"),
        ) {
            (Ok(ae), Ok(be)) => match (reopened_a.get_attr(ae.ino), reopened_b.get_attr(be.ino)) {
                (Ok(a_attr), Ok(b_attr)) => {
                    ae.ino == expected.ino
                        && be.ino == expected.ino
                        && a_attr.kind == expected_attr.kind
                        && b_attr.kind == expected_attr.kind
                }
                _ => false,
            },
            _ => false,
        }
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_concurrent_rename_single_winner() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-concurrent-rename");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");
    a.create_dir(ROOT_INO, b"source.txt", 0, 0, 0o755)
        .expect("seed source");
    a.flush_metadata().expect("flush source");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"source.txt",
        Duration::from_secs(10)
    ));

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let a2 = Arc::clone(&a);
    let b2 = Arc::clone(&b);
    let barrier_a = Arc::clone(&barrier);
    let barrier_b = Arc::clone(&barrier);

    let t1 = std::thread::spawn(move || {
        barrier_a.wait();
        let out = a2.rename(ROOT_INO, b"source.txt", ROOT_INO, b"moved_by_a.txt");
        if out.is_ok() {
            let _ = a2.flush_metadata();
        }
        out
    });
    let t2 = std::thread::spawn(move || {
        barrier_b.wait();
        let out = b2.rename(ROOT_INO, b"source.txt", ROOT_INO, b"moved_by_b.txt");
        if out.is_ok() {
            let _ = b2.flush_metadata();
        }
        out
    });

    let r1 = t1.join().expect("thread a");
    let r2 = t2.join().expect("thread b");
    let ok_count = [r1.is_ok(), r2.is_ok()].into_iter().filter(|v| *v).count();
    assert!(ok_count >= 1, "at least one rename should apply");
    assert!(wait_until(Duration::from_secs(12), || {
        let _ = a.sync_metadata();
        a.lookup(ROOT_INO, b"source.txt").is_err()
    }));
    let a_exists = a.lookup(ROOT_INO, b"moved_by_a.txt").is_ok();
    let b_exists = a.lookup(ROOT_INO, b"moved_by_b.txt").is_ok();
    assert_ne!(a_exists, b_exists, "only one destination name should exist");
    assert!(
        a.lookup(ROOT_INO, b"source.txt").is_err(),
        "source must be gone"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_reopen_after_remote_rename_overwrite_recovers_without_discover() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-reopen-rename-overwrite");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let src = create_published_small_file(&a, ROOT_INO, b"src.txt", 50_310);
    let dst = create_published_small_file(&a, ROOT_INO, b"dst.txt", 50_311);
    a.flush_metadata().expect("flush seed files");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"src.txt",
        Duration::from_secs(10)
    ));
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"dst.txt",
        Duration::from_secs(10)
    ));

    a.rename(ROOT_INO, b"src.txt", ROOT_INO, b"dst.txt")
        .expect("rename overwrite on a");
    a.flush_metadata().expect("flush rename overwrite");
    assert!(wait_until(Duration::from_secs(10), || {
        let _ = b.sync_metadata();
        b.lookup(ROOT_INO, b"src.txt").is_err()
            && b.lookup(ROOT_INO, b"dst.txt")
                .map(|entry| entry.ino == src.ino)
                .unwrap_or(false)
    }));

    drop(b);
    let reopened_b = reopen_pg_engine_with_retry(&db_b);
    assert!(wait_until(Duration::from_secs(12), || {
        let _ = reopened_b.sync_metadata();
        reopened_b.lookup(ROOT_INO, b"src.txt").is_err()
            && reopened_b
                .lookup(ROOT_INO, b"dst.txt")
                .map(|entry| entry.ino == src.ino)
                .unwrap_or(false)
            && reopened_b.get_attr(dst.ino).is_err()
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_reopen_after_publish_before_local_persist_recovers_without_discover() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _poll_guard = set_test_env("STUDIOFS_PG_SYNC_POLL_SECS", "3600");

    let dir = tempdir().expect("tempdir");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-crash-after-publish");
    let info = volume.format_info();
    seed_local(&db, &info);

    let a = PgMetaEngine::open(&db).expect("open a");
    a.create_file(ROOT_INO, b"draft.txt", 0, 0, 0o644)
        .expect("create local file");

    let start = a.flushed_event_seq.load(Ordering::SeqCst);
    let end = a.local.get_counter("nextEventSeq").expect("nextEventSeq");
    let events = a.local.read_events_range(start, end).expect("local events");
    let _version = a
        .push_event_log_batch_locked(&events)
        .expect("push batch without local persist");

    drop(a);

    let reopened = reopen_pg_engine_with_retry(&db);
    assert!(wait_until(Duration::from_secs(12), || {
        let _ = reopened.sync_metadata();
        reopened.lookup(ROOT_INO, b"draft.txt").is_ok()
            && reopened.flushed_event_seq.load(Ordering::SeqCst)
                == reopened.local.get_counter("nextEventSeq").unwrap_or(0)
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_reopen_after_publish_then_remote_unlink_recovers_without_discover() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _poll_guard = set_test_env("STUDIOFS_PG_SYNC_POLL_SECS", "3600");

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-crash-publish-then-unlink");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = a
        .create_dir(ROOT_INO, b"remote.txt", 0, 0, 0o755)
        .expect("create local entry");
    let start = a.flushed_event_seq.load(Ordering::SeqCst);
    let end = a.local.get_counter("nextEventSeq").expect("nextEventSeq");
    let events = a.local.read_events_range(start, end).expect("local events");
    let _version = a
        .push_event_log_batch_locked(&events)
        .expect("push batch without local persist");

    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"remote.txt",
        Duration::from_secs(12)
    ));
    b.rmdir(ROOT_INO, b"remote.txt").expect("peer remove");
    b.flush_metadata().expect("flush peer remove");

    drop(a);

    let reopened_a = reopen_pg_engine_with_retry(&db_a);
    assert!(wait_until(Duration::from_secs(12), || {
        let _ = reopened_a.sync_metadata();
        reopened_a.lookup(ROOT_INO, b"remote.txt").is_err()
            && reopened_a.get_attr(file.ino).is_err()
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_reopen_after_unpublished_local_create_flushes_without_discover() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-reopen-unpublished-create");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    a.create_dir(ROOT_INO, b"draft.txt", 0, 0, 0o755)
        .expect("create local draft");
    drop(a);

    let reopened_a = reopen_pg_engine_with_retry(&db_a);
    assert!(
        reopened_a.replication_backlog().pending_ops > 0,
        "reopen must rebuild pending state for unpublished local events"
    );

    reopened_a
        .flush_metadata()
        .expect("flush restored unpublished events");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"draft.txt",
        Duration::from_secs(12)
    ));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_reopen_after_claimed_batch_crash_flushes_without_discover() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _poll_guard = set_test_env("STUDIOFS_PG_SYNC_POLL_SECS", "3600");

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-reopen-claimed-batch-crash");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    a.create_dir(ROOT_INO, b"draft.txt", 0, 0, 0o755)
        .expect("create local draft");
    let claimed = a.claim_pending_batch(true).expect("claim pending batch");
    assert!(claimed.event_end > claimed.event_start);
    drop(a);

    let reopened_a = reopen_pg_engine_with_retry(&db_a);
    assert!(
        reopened_a.replication_backlog().pending_ops > 0,
        "reopen must recover a claimed-but-unpublished batch after crash"
    );

    reopened_a.flush_metadata().expect("flush recovered batch");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"draft.txt",
        Duration::from_secs(12)
    ));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_reopen_after_claimed_multi_event_batch_preserves_sequence_without_discover() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let _poll_guard = set_test_env("STUDIOFS_PG_SYNC_POLL_SECS", "3600");

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-reopen-claimed-multievent");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let dir = a
        .create_dir(ROOT_INO, b"drafts", 0, 0, 0o755)
        .expect("create drafts dir");
    let file = a
        .create_dir(dir.ino, b"temp.txt", 0, 0, 0o755)
        .expect("create temp dir");
    a.rename(dir.ino, b"temp.txt", dir.ino, b"final.txt")
        .expect("rename temp to final");
    let claimed = a.claim_pending_batch(true).expect("claim pending batch");
    assert!(claimed.event_end > claimed.event_start);
    drop(a);

    let reopened_a = reopen_pg_engine_with_retry(&db_a);
    reopened_a
        .flush_metadata()
        .expect("flush recovered multi-event batch");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = b.sync_metadata();
        match b.lookup(ROOT_INO, b"drafts") {
            Ok(dir_entry) => {
                b.lookup(dir_entry.ino, b"temp.txt").is_err()
                    && b.lookup(dir_entry.ino, b"final.txt")
                        .map(|entry| entry.ino == file.ino)
                        .unwrap_or(false)
            }
            Err(_) => false,
        }
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_delete_and_recreate() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-delete-recreate");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let first = a
        .create_dir(ROOT_INO, b"recreate.txt", 0, 0, 0o755)
        .expect("create first");
    a.rmdir(ROOT_INO, b"recreate.txt").expect("remove first");
    let second = a
        .create_dir(ROOT_INO, b"recreate.txt", 0, 0, 0o755)
        .expect("create second");
    assert_ne!(
        first.ino, second.ino,
        "recreated entry should have a new inode in current model"
    );

    a.flush_metadata().expect("flush a");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"recreate.txt",
        Duration::from_secs(10)
    ));
    let on_b = b.lookup(ROOT_INO, b"recreate.txt").expect("lookup on b");
    assert_eq!(on_b.ino, second.ino);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_rapid_create_delete() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = tempdir().expect("tempdir");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-rapid-cd");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    for i in 0..200 {
        let name = format!("rapid_{i:03}.tmp");
        pg.create_file(ROOT_INO, name.as_bytes(), 0, 0, 0o644)
            .expect("create");
        pg.unlink(ROOT_INO, name.as_bytes()).expect("unlink");
    }

    let entries = pg.readdir(ROOT_INO).expect("readdir root");
    assert!(
        entries.is_empty(),
        "rapid create/delete should leave root empty"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_hardlinks_cross_client() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-hardlink");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = create_published_small_file(&a, ROOT_INO, b"orig.txt", 50_312);
    a.link(file.ino, ROOT_INO, b"link.txt").expect("hardlink");

    a.flush_metadata().expect("flush a");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"orig.txt",
        Duration::from_secs(10)
    ));
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"link.txt",
        Duration::from_secs(10)
    ));
    let orig = b.lookup(ROOT_INO, b"orig.txt").expect("lookup orig");
    let link = b.lookup(ROOT_INO, b"link.txt").expect("lookup link");
    assert_eq!(orig.ino, link.ino, "hardlink should point to same inode");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_symlinks_cross_client() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-symlink");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let sym = a
        .create_symlink(ROOT_INO, b"my-link", b"target/path.txt", 0, 0)
        .expect("create symlink");
    a.flush_metadata().expect("flush a");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"my-link",
        Duration::from_secs(10)
    ));
    let link = b.lookup(ROOT_INO, b"my-link").expect("lookup symlink");
    assert_eq!(link.ino, sym.ino);
    let target = b.get_symlink_target(link.ino).expect("readlink");
    assert_eq!(target, b"target/path.txt");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_peer_can_update_foreign_visible_symlink_target() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-peer-symlink-update");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let sym = a
        .create_symlink(ROOT_INO, b"shared-link", b"target/a", 0, 0)
        .expect("create symlink on a");
    a.flush_metadata().expect("flush create on a");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"shared-link",
        Duration::from_secs(10)
    ));
    assert_eq!(b.get_symlink_target(sym.ino).unwrap(), b"target/a");

    b.set_symlink_target(sym.ino, b"target/b")
        .expect("update symlink on b");
    b.flush_metadata().expect("flush update on b");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = a.sync_metadata();
        a.get_symlink_target(sym.ino)
            .map(|target| target == b"target/b")
            .unwrap_or(false)
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_local_xattr_overlay_masks_foreign_update_until_flush() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-xattr-overlay-precedence");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = create_published_small_file(&a, ROOT_INO, b"shared.txt", 50_313);
    a.set_xattr(file.ino, b"user.color", b"red", 0)
        .expect("set red xattr on a");
    a.flush_metadata().expect("flush red on a");
    assert!(wait_visible(
        &b,
        ROOT_INO,
        b"shared.txt",
        Duration::from_secs(10)
    ));
    assert_eq!(
        b.get_xattr(file.ino, b"user.color").unwrap(),
        Some(b"red".to_vec())
    );

    b.set_xattr(file.ino, b"user.color", b"blue", 0)
        .expect("set blue xattr on b");

    a.set_xattr(file.ino, b"user.color", b"green", 0)
        .expect("set green xattr on a");
    a.flush_metadata().expect("flush green on a");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = b.sync_metadata();
        b.get_xattr(file.ino, b"user.color")
            .map(|value| value == Some(b"blue".to_vec()))
            .unwrap_or(false)
    }));
    assert_eq!(
        b.get_xattr(file.ino, b"user.color").unwrap(),
        Some(b"blue".to_vec()),
        "local xattr overlay should mask foreign base update"
    );

    b.flush_metadata().expect("flush blue on b");

    assert!(wait_until(Duration::from_secs(12), || {
        let _ = a.sync_metadata();
        a.get_xattr(file.ino, b"user.color")
            .map(|value| value == Some(b"blue".to_vec()))
            .unwrap_or(false)
    }));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_basic_crud() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = tempdir().expect("tempdir");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-basic-crud");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    let file = pg
        .create_file(ROOT_INO, b"hello.txt", 0, 0, 0o644)
        .expect("create");
    let looked = pg.lookup(ROOT_INO, b"hello.txt").expect("lookup");
    assert_eq!(file.ino, looked.ino);

    pg.rename(ROOT_INO, b"hello.txt", ROOT_INO, b"renamed.txt")
        .expect("rename");
    assert!(pg.lookup(ROOT_INO, b"hello.txt").is_err());
    assert!(pg.lookup(ROOT_INO, b"renamed.txt").is_ok());

    pg.unlink(ROOT_INO, b"renamed.txt").expect("unlink");
    assert!(pg.lookup(ROOT_INO, b"renamed.txt").is_err());

    pg.create_dir(ROOT_INO, b"subdir", 0, 0, 0o755)
        .expect("mkdir");
    pg.rmdir(ROOT_INO, b"subdir").expect("rmdir");
    assert!(pg.lookup(ROOT_INO, b"subdir").is_err());
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_deep_path() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = tempdir().expect("tempdir");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-deep-path");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    let mut parent = ROOT_INO;
    for n in [b"a", b"b", b"c", b"d", b"e"] {
        let dir = pg.create_dir(parent, n, 0, 0, 0o755).expect("mkdir");
        parent = dir.ino;
    }
    let leaf = pg
        .create_file(parent, b"leaf.txt", 0, 0, 0o644)
        .expect("create leaf");
    let looked = pg.lookup(parent, b"leaf.txt").expect("lookup leaf");
    assert_eq!(leaf.ino, looked.ino);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_large_directory() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = tempdir().expect("tempdir");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-large-dir");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    for i in 0..1000 {
        let name = format!("file_{i:04}.dat");
        pg.create_file(ROOT_INO, name.as_bytes(), 0, 0, 0o644)
            .expect("create");
    }
    let entries = pg.readdir(ROOT_INO).expect("readdir");
    assert_eq!(entries.len(), 1000);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_special_filenames() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = tempdir().expect("tempdir");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-special-names");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    let names = vec![
        b"file with spaces.txt".to_vec(),
        "файл_тест_日本語.txt".as_bytes().to_vec(),
        format!("{}.txt", "a".repeat(200)).as_bytes().to_vec(),
        b"file...multiple...dots.txt".to_vec(),
        b"-dashstart.txt".to_vec(),
    ];
    for n in &names {
        pg.create_file(ROOT_INO, n, 0, 0, 0o644).expect("create");
    }
    for n in &names {
        assert!(
            pg.lookup(ROOT_INO, n).is_ok(),
            "name should resolve: {:?}",
            n
        );
    }
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_readdir_during_modifications() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-readdir-race");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let writer = std::thread::spawn({
        let a = Arc::clone(&a);
        move || {
            for i in 0..120 {
                let name = format!("w_{i:03}.dat");
                let _ = a.create_file(ROOT_INO, name.as_bytes(), 0, 0, 0o644);
                if i % 3 == 0 {
                    let _ = a.unlink(ROOT_INO, name.as_bytes());
                }
            }
        }
    });

    for _ in 0..80 {
        let _ = b.sync_metadata();
        let _ = b.readdir(ROOT_INO).expect("readdir should not fail");
        std::thread::sleep(Duration::from_millis(10));
    }
    writer.join().expect("writer join");
    b.sync_metadata().expect("final sync");
    let _ = b.readdir(ROOT_INO).expect("final readdir");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_visibility_timing() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-vis-timing");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let mut delays = Vec::new();
    for i in 0..5 {
        let name = format!("vis_{i:02}.dat");
        a.create_dir(ROOT_INO, name.as_bytes(), 0, 0, 0o755)
            .expect("create");
        a.flush_metadata().expect("flush a");
        let start = std::time::Instant::now();
        let mut seen = false;
        for _ in 0..50 {
            b.sync_metadata().expect("sync b");
            if b.lookup(ROOT_INO, name.as_bytes()).is_ok() {
                seen = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(seen, "entry should become visible on peer");
        delays.push(start.elapsed().as_millis() as u64);
    }

    let avg = delays.iter().sum::<u64>() / delays.len() as u64;
    assert!(avg < 5000, "avg visibility delay should stay bounded");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_snapshot_eventually_visible_and_mountable_on_peer() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-snapshot-visibility");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = a
        .create_file(ROOT_INO, b"snap-peer.txt", 0, 0, 0o644)
        .expect("create file on a");
    set_file_state(&a, file.ino, 64, 1);
    a.stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(44_001, 64))
        .expect("set payload slice");

    let created = a.create_snapshot(ROOT_INO).expect("create snapshot");
    a.flush_metadata().expect("flush snapshot");

    let visible = wait_until(Duration::from_secs(12), || {
        let _ = b.sync_metadata();
        b.get_snapshot(&created.id)
            .ok()
            .flatten()
            .map(|s| s.state == SnapshotState::Synced)
            .unwrap_or(false)
    });
    assert!(visible, "snapshot should become visible on peer");

    let peer_snapshot = b
        .get_snapshot(&created.id)
        .expect("snapshot query")
        .expect("snapshot exists on peer");
    assert!(peer_snapshot.pinned_object_ids.contains(&44_001));
    assert!(!peer_snapshot.snapshot_payload.is_empty());

    let mount_dir = tempdir().expect("mount tempdir");
    let mount_db = mount_dir.path().join("snapshot-mount.redb");
    let mount_engine = RedbMetaEngine::open(&mount_db).expect("open mount redb");
    let mut mount_info = info.clone();
    mount_info.metadata.backend = MetadataBackend::RedbLocal;
    mount_info.metadata.postgres = None;
    mount_engine
        .set_format_info(&mount_info)
        .expect("set mount format");
    let snap_state = RedbMetaEngine::decode_snapshot_payload(&peer_snapshot.snapshot_payload)
        .expect("decode payload");
    mount_engine
        .import_snapshot(&snap_state)
        .expect("import payload");
    assert!(mount_engine.lookup(ROOT_INO, b"snap-peer.txt").is_ok());
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_janitor_lock_is_exclusive() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-janitor-lock");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    assert!(
        a.try_acquire_global_janitor_lock()
            .expect("janitor lock acquire on a"),
        "first janitor should acquire the PG lock"
    );
    assert!(
        !b.try_acquire_global_janitor_lock()
            .expect("janitor lock probe on b while a holds it"),
        "janitor should skip when another client already holds the PG lock"
    );
    a.release_global_janitor_lock()
        .expect("janitor lock release on a");

    assert!(
        b.try_acquire_global_janitor_lock()
            .expect("janitor lock acquire on b after release"),
        "janitor should proceed once the PG lock is free again"
    );
    b.release_global_janitor_lock()
        .expect("janitor lock release on b");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_janitor_reads_snapshot_pins_from_postgres() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-janitor-snapshot-pins");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = a
        .create_file(ROOT_INO, b"snap-gc.txt", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&a, file.ino, 64, 1);
    a.stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(55_001, 64))
        .expect("set payload slice");
    let _snapshot = a.create_snapshot(ROOT_INO).expect("create snapshot");
    a.flush_metadata().expect("flush snapshot");

    let pins = b
        .get_global_snapshot_pins()
        .expect("query snapshot pins from postgres");
    assert!(
        pins.contains(&55_001),
        "janitor must see cross-client snapshot pins from postgres"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_uuid_mismatch_blocks_consistency_check() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-uuid-mismatch");
    let info = volume.format_info();
    seed_local(&db_path, &info);

    let engine = PgMetaEngine::open(&db_path).expect("open engine");

    // Initial check should pass
    engine
        .check_consistency()
        .expect("initial UUID check must pass");

    // Simulate disk recreation: change UUID in PG for this volume_id
    volume.update_uuid("recreated-uuid-12345");

    // Next check should fail
    let result = engine.check_consistency();
    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::UuidMismatch { local, remote } => {
            assert_eq!(local, info.uuid);
            assert_eq!(remote, "recreated-uuid-12345");
        }
        e => panic!("expected UuidMismatch, got {:?}", e),
    }
}
